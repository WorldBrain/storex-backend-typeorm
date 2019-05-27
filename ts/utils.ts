import { CollectionDefinition, StorageRegistry } from "@worldbrain/storex";

export type ObjectCleaner = (object : any, options : { collectionDefinition : CollectionDefinition }) => any
export type ObjectCleanerOptions = { collectionDefinition : CollectionDefinition }

export function makeCleanerChain(cleaners : ObjectCleaner[]) : ObjectCleaner {
    return (object : any, options : ObjectCleanerOptions) => {
        for (const cleaner of cleaners) {
            object = cleaner(object, options)
        }
        return object
    }
}

export function makeCleanBooleanFieldsForRead(options : { storageRegistry : StorageRegistry }) {
    const fieldsByCollection : {[collection : string] : string[]} = {}

    for (const [collectionName, collectionDefinition] of Object.entries(options.storageRegistry.collections)) {
        for (const [fieldName, fieldDefinition] of Object.entries(collectionDefinition.fields)) {
            if (fieldDefinition.type === 'boolean') {
                fieldsByCollection[collectionName] = fieldsByCollection[collectionName] || []
                fieldsByCollection[collectionName].push(fieldName)
            }
        }
    }

    return function cleanBooleanFieldsForRead(object : any, options : ObjectCleanerOptions) {
        const fieldNames = fieldsByCollection[options.collectionDefinition.name!]
        for (const fieldName of fieldNames || []) {
            object[fieldName] = !!object[fieldName]
        }
        return object
    }
}

export function cleanOptionalFieldsForRead(object : any, options : ObjectCleanerOptions) {
    for (const [fieldName, fieldDefinition] of Object.entries(options.collectionDefinition.fields)) {
        if (fieldDefinition.optional && object[fieldName] === null) {
            delete object[fieldName]
        }
    }

    return object
}
