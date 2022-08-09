import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { BigQuery, Json, Table, TableField, TableMetadata } from '@google-cloud/bigquery'

let latestSchema: Object = {} // holding the global schema fields

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table
        bigQueryTableFields: TableField[]

        exportEventsBuffer: ReturnType<typeof createBuffer>
        exportEventsToIgnore: Set<string>
        exportEventsWithRetry: (payload: UploadJobPayload, meta: PluginMeta<BigQueryPlugin>) => Promise<void>
    }
    config: {
        datasetId: string
        tableId: string

        exportEventsBufferBytes: string
        exportEventsBufferSeconds: string
        exportEventsToIgnore: string
        exportElementsOnAnyEvent: 'Yes' | 'No'
    }
    jobs: {
        exportEventsWithRetry: UploadJobPayload
    }
}>

interface UploadJobPayload {
    batch: PluginEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const setupPlugin: BigQueryPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config } = meta
    if (!attachments.googleCloudKeyJson) {
        throw new Error('JSON config not provided!')
    }
    if (!config.datasetId) {
        throw new Error('Dataset ID not provided!')
    }
    if (!config.tableId) {
        throw new Error('Table ID not provided!')
    }

    const credentials = JSON.parse(attachments.googleCloudKeyJson.contents.toString())
    global.bigQueryClient = new BigQuery({
        projectId: credentials['project_id'],
        credentials,
        autoRetry: false,
    })
    global.bigQueryTable = global.bigQueryClient.dataset(config.datasetId).table(config.tableId)

    global.bigQueryTableFields = [
        { name: 'uuid', type: 'STRING' },
        { name: 'event', type: 'STRING' },
        { name: 'properties', type: 'STRING' },
        { name: 'elements', type: 'STRING' },
        { name: 'set', type: 'STRING' },
        { name: 'set_once', type: 'STRING' },
        { name: 'distinct_id', type: 'STRING' },
        { name: 'team_id', type: 'INT64' },
        { name: 'ip', type: 'STRING' },
        { name: 'site_url', type: 'STRING' },
        { name: 'timestamp', type: 'TIMESTAMP' },
        { name: 'bq_ingested_timestamp', type: 'TIMESTAMP' },
    ]

    try {
        const [metadata]: TableMetadata[] = await global.bigQueryTable.getMetadata()

        if (!metadata.schema || !metadata.schema.fields) {
            // noinspection ExceptionCaughtLocallyJS
            throw new Error('Can not get metadata for table. Please check if the table schema is defined.')
        }

        const existingFields = metadata.schema.fields
        const fieldsToAdd = global.bigQueryTableFields.filter(
            ({ name }) => !existingFields.find((f: any) => f.name === name),
        )

        if (fieldsToAdd.length > 0) {
            console.info(
                `Incomplete schema on BigQuery table! Adding the following fields to reach parity: ${JSON.stringify(
                    fieldsToAdd,
                )}`,
            )

            let result: TableMetadata
            try {
                metadata.schema.fields = metadata.schema.fields.concat(fieldsToAdd)
                ;[result] = await global.bigQueryTable.setMetadata(metadata)
            } catch (error) {
                const fieldsToStillAdd = global.bigQueryTableFields.filter(
                    ({ name }) => !result.schema?.fields?.find((f: any) => f.name === name),
                )

                if (fieldsToStillAdd.length > 0) {
                    // noinspection ExceptionCaughtLocallyJS
                    throw new Error(
                        `Tried adding fields ${JSON.stringify(fieldsToAdd)}, but ${JSON.stringify(
                            fieldsToStillAdd,
                        )} still to add. Can not start plugin.`,
                    )
                }
            }
        }
    } catch (error) {
        // some other error? abort!
        if (!error.message.includes('Not found')) {
            throw new Error(error)
        }
        console.log(`Creating BigQuery Table - ${config.datasetId}:${config.tableId}`)

        try {
            await global.bigQueryClient
                .dataset(config.datasetId)
                .createTable(config.tableId, { schema: global.bigQueryTableFields })
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('Already Exists')) {
                throw error
            }
        }
    }

    setupBufferExportCode(meta, exportEventsToBigQuery)
}

export async function exportEventsToBigQuery(events: PluginEvent[], { global, config }: PluginMeta<BigQueryPlugin>) {
    const insertOptions = {
        createInsertId: false,
        partialRetries: 0,
        raw: true,
    }

    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }
    try {
        let eventFields: Array<{ name: string, type: string }> = []
        let eventFieldKeys: Array<string> = []
        const rows = events.map((event) => {
            const {
                event: eventName,
                properties,
                $set,
                $set_once,
                distinct_id,
                team_id,
                site_url,
                now,
                sent_at,
                uuid,
                elements,
                ..._discard
            } = event
            const ip = properties?.['$ip'] || event.ip
            const timestamp = event.timestamp || properties?.timestamp || now || sent_at
            let ingestedProperties = properties
            const flattenProperties = __flatten_object(properties)
            console.log('Flattern Properties', flattenProperties)
            Object.entries(flattenProperties).forEach(([key, value], index) => {
                if (eventFields.filter(e => e.name === key).length == 0) {
                    eventFields.push({ name: key, type: 'STRING' })
                    eventFieldKeys.push(key)
                }
            })

            const shouldExportElementsForEvent =
                eventName === '$autocapture' || config.exportElementsOnAnyEvent === 'Yes'


            const object: { json: Record<string, any>; insertId?: string } = {
                json: {
                    uuid,
                    event: eventName,
                    properties: JSON.stringify(ingestedProperties || {}),
                    elements: JSON.stringify(shouldExportElementsForEvent && elements ? elements : {}),
                    set: JSON.stringify($set || {}),
                    set_once: JSON.stringify($set_once || {}),
                    distinct_id,
                    team_id,
                    ip,
                    site_url,
                    timestamp: timestamp,
                    bq_ingested_timestamp: new Date().toISOString(),
                    ...flattenProperties,
                },
            }
            return object
        })
        if (eventFieldKeys.length > 0) {
            eventFieldKeys.forEach(function(eventFieldName) {
                Object.entries(rows).forEach(([key, value], index) => {
                    if (Object.keys(value.json).indexOf(eventFieldName) == -1) {
                        value.json[eventFieldName] = null
                    }
                })
            })
        }
        const start = Date.now()
        await __sync_new_fields(eventFields, global, config) // sync new keys

        await global.bigQueryTable.insert(rows, insertOptions)
        const end = Date.now() - start

        console.log(
            `Inserted ${events.length} ${events.length > 1 ? 'events' : 'event'} to BigQuery. Took ${
                end / 1000
            } seconds.`,
        )
    } catch (error) {
        console.error(
            `Error inserting ${events.length} ${events.length > 1 ? 'events' : 'event'} into BigQuery: `,
            error,
        )
        throw new RetryError(`Error inserting into BigQuery! ${JSON.stringify(error.errors)}`)
    }
}

// What follows is code that should be abstracted away into the plugin server itself.

const setupBufferExportCode = (
    meta: PluginMeta<BigQueryPlugin>,
    exportEvents: (events: PluginEvent[], meta: PluginMeta<BigQueryPlugin>) => Promise<void>,
) => {
    const uploadBytes = Math.max(
        1024 * 1024,
        Math.min(parseInt(meta.config.exportEventsBufferBytes) || 1024 * 1024, 1024 * 1024 * 10),
    )
    const uploadSeconds = Math.max(1, Math.min(parseInt(meta.config.exportEventsBufferSeconds) || 30, 600))

    meta.global.exportEventsToIgnore = new Set(
        meta.config.exportEventsToIgnore
            ? meta.config.exportEventsToIgnore.split(',').map((event) => event.trim())
            : null,
    )
    meta.global.exportEventsBuffer = createBuffer({
        limit: uploadBytes,
        timeoutSeconds: uploadSeconds,
        onFlush: async (batch) => {
            const jobPayload = {
                batch,
                batchId: Math.floor(Math.random() * 1000000),
                retriesPerformedSoFar: 0,
            }
            const firstThroughQueue = false // TODO: might make sense sometimes? e.g. when we are processing too many tasks already?
            if (firstThroughQueue) {
                await meta.jobs.exportEventsWithRetry(jobPayload).runNow()
            } else {
                await meta.global.exportEventsWithRetry(jobPayload, meta)
            }
        },
    })
    meta.global.exportEventsWithRetry = async (payload: UploadJobPayload, meta: PluginMeta<BigQueryPlugin>) => {
        const { jobs } = meta
        try {
            await exportEvents(payload.batch, meta)
        } catch (err) {
            if (err instanceof RetryError) {
                if (payload.retriesPerformedSoFar < 15) {
                    const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
                    console.log(`Enqueued batch ${payload.batchId} for retry in ${Math.round(nextRetrySeconds)}s`)

                    await jobs
                        .exportEventsWithRetry({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
                        .runIn(nextRetrySeconds, 'seconds')
                } else {
                    console.log(
                        `Dropped batch ${payload.batchId} after retrying ${payload.retriesPerformedSoFar} times`,
                    )
                }
            } else {
                throw err
            }
        }
    }
}

export const jobs: BigQueryPlugin['jobs'] = {
    exportEventsWithRetry: async (payload, meta) => {
        await meta.global.exportEventsWithRetry(payload, meta)
    },
}

export const onEvent: BigQueryPlugin['onEvent'] = (event, { global }) => {
    if (!global.exportEventsToIgnore.has(event.event)) {
        global.exportEventsBuffer.add(event, JSON.stringify(event).length)
    }
}

async function __sync_new_fields(eventFields: TableField[], global: any, config: any): Promise<void> {
    console.log('Global fields Array:', global.bigQueryTableFields)
    console.log('latestSchema Array:', latestSchema)
    try {
        if (JSON.stringify(latestSchema) === '{}') {
            // we'll fetch schema details from metadata for the first time
            const [metadata]: TableMetadata[] = await global.bigQueryTable.getMetadata()
            if (!metadata.schema || !metadata.schema.fields) {
                // noinspection ExceptionCaughtLocallyJS
                throw new Error('Can not get metadata for table. Please check if the table schema is defined.')
            }
            metadata.schema.fields.forEach((value) => {
                // @ts-ignore
                latestSchema[value.name] = value.type  // populate fields hashmap for existing fields
            })
        }

        // let's find non-existing fields
        const fieldsToAdd = eventFields.filter(
            ({ name }) => !latestSchema.hasOwnProperty(name),
        )
        if (fieldsToAdd.length > 0) {
            console.info(
                `Incomplete schema on BigQuery table! Adding the following fields to reach parity: ${JSON.stringify(
                    fieldsToAdd,
                )}`,
            )

            let result: TableMetadata

            // since there are new fields to add, let's fetch the metadata to sync new fields
            const [metadata]: TableMetadata[] = await global.bigQueryTable.getMetadata()
            if (!metadata.schema || !metadata.schema.fields) {
                // noinspection ExceptionCaughtLocallyJS
                throw new Error('Can not get metadata for table. Please check if the table schema is defined.')
            }
            try {
                metadata.schema.fields = metadata.schema.fields.concat(fieldsToAdd)
                ;[result] = await global.bigQueryTable.setMetadata(metadata)

                // add to latestSchema again
                fieldsToAdd.forEach((value) => {
                    // @ts-ignore
                    latestSchema[value.name] = value.type
                })
            } catch (error) {
                const fieldsToStillAdd = global.bigQueryTableFields.filter(
                    ({ name }) => !result.schema?.fields?.find((f: any) => f.name === name),
                )

                if (fieldsToStillAdd.length > 0) {
                    // noinspection ExceptionCaughtLocallyJS
                    throw new Error(
                        `Tried adding fields ${JSON.stringify(fieldsToAdd)}, but ${JSON.stringify(
                            fieldsToStillAdd,
                        )} still to add. Can not start plugin.`,
                    )
                }
            }
        }
    } catch (error) {
        // some other error? abort!
        if (!error.message.includes('Not found')) {
            throw new Error(error)
        }
        console.log(`Creating BigQuery Table - ${config.datasetId}:${config.tableId}`)

        try {
            await global.bigQueryClient
                .dataset(config.datasetId)
                .createTable(config.tableId, { schema: global.bigQueryTableFields })
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('Already Exists')) {
                throw error
            }
        }
    }
}

function __flatten_object(obj:any) {
    let result:any = {}
    for (const i in obj) {
        if (!obj.hasOwnProperty(i)) continue
        if ((typeof obj[i]) === 'object' && !Array.isArray(obj[i])) {
            const temp = __flatten_object(obj[i])
            for (const j in temp) {
                if (i.includes('$'))
                    continue
                result[(i + '__' + j).replace(/\$/g, '')] = Array.isArray(temp[j]) ? JSON.stringify(temp[j]) : temp[j]
            }
        }else if (Array.isArray(obj[i])) {
            obj[i].forEach((value, index) => {
                const temp = __flatten_object(value)
                // for (const j in temp) {
                //     if (i.includes('$'))
                //         continue
                //     result[i + 'ddddd' + (index + 'ddddd' + j).replace(/\$/g, '')] = Array.isArray(temp[j]) ? JSON.stringify(temp[j]) : temp[j]
                // }
            })

        } else {
            if (i.includes('$'))
                continue
            result[i.replace(/\$/, '')] = Array.isArray(obj[i]) ? JSON.stringify(obj[i]) : obj[i]
        }
    }
    return result
}
