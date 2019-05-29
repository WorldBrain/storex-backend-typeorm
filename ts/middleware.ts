import { StorageRegistry, OperationBatch } from "@worldbrain/storex";
import { StorageMiddleware, StorageMiddlewareContext } from "@worldbrain/storex/lib/types/middleware";
import { dissectCreateObjectOperation, convertCreateObjectDissectionToBatch, CreateObjectDissection, setIn } from '@worldbrain/storex/lib/utils'

export class ComplexCreateMiddleware implements StorageMiddleware {
    constructor(private options : { storageRegistry : StorageRegistry }) {
    }

    async process(context : StorageMiddlewareContext) {
        const operationName = context.operation[0]
        if (operationName === 'createObject') {
            return this._processCreateObject(context)
        } else if (operationName === 'executeBatch') {
            return this._processExecuteBatch(context)
        } else {
            return context.next.process({ operation: context.operation })
        }
    }

    async _processCreateObject(context : StorageMiddlewareContext) {
        const [collection, object] = context.operation.slice(1)

        let placeholderCount = 0
        const dissection = dissectCreateObjectOperation({ operation: 'createObject', collection, args: object }, this.options.storageRegistry, {
            generatePlaceholder: () => `object-${++placeholderCount}`
        })
        const batchToExecute = convertCreateObjectDissectionToBatch(dissection)
        const batchResult = await context.next.process({ operation: ['executeBatch', batchToExecute] })
        this._reconstructCreatedObject(object, collection, dissection, batchResult.info)
        return { object }
    }

    async _processExecuteBatch(context : StorageMiddlewareContext) {
        const newBatch : OperationBatch = []
        const placeholders : {[placeholderName : string] : {
            collection : string
            dissection : CreateObjectDissection
            originalObject : any
        }} = {}
        for (const step of context.operation[1] as OperationBatch) {
            if (step.operation !== 'createObject' || !step.placeholder) {
                newBatch.push(step)
                continue
            }
            
            let placeholderCount = 0
            const dissection = dissectCreateObjectOperation({
                operation: 'createObject',
                collection: step.collection,
                args: step.args
            }, this.options.storageRegistry, {
                generatePlaceholder: () => `${step.placeholder}-${++placeholderCount}`
            })
            const batchToExecute = convertCreateObjectDissectionToBatch(dissection)    

            placeholders[step.placeholder] = {
                collection: step.collection,
                dissection,
                originalObject: step.args
            }
            newBatch.push(...batchToExecute)
        }

        const rawBatchResult = await context.next.process({ operation: ['executeBatch', newBatch] })
        
        const processedBatchResult = { info: {} }
        for (const [placeholderName, placeholderInfo] of Object.entries(placeholders)) {
            this._reconstructCreatedObject(placeholderInfo.originalObject, placeholderInfo.collection, placeholderInfo.dissection, rawBatchResult.info)
            processedBatchResult.info[placeholderName] = { object: placeholderInfo.originalObject }
        }

        return processedBatchResult
    }

    async _reconstructCreatedObject(object : any, collection : string, operationDissection : CreateObjectDissection, batchResultInfo : any) {
        for (const step of operationDissection.objects) {
            const collectionDefiniton = this.options.storageRegistry.collections[collection]
            const pkIndex = collectionDefiniton.pkIndex
            setIn(object, [...step.path, pkIndex], batchResultInfo[step.placeholder].object[pkIndex as string])
        }
    }
}
