const jsonStringify = require('json-stable-stringify')
import {
    CollectionDefinition,
    StorageRegistry,
    isChildOfRelationship,
    isConnectsRelationship,
} from '@worldbrain/storex'
import { ConnectionOptions } from 'typeorm'

export type ObjectCleaner = (
    object: any,
    options: { collectionDefinition: CollectionDefinition },
) => any
export type ObjectCleanerOptions = {
    collectionDefinition: CollectionDefinition
}

export function makeCleanerChain(cleaners: ObjectCleaner[]): ObjectCleaner {
    return (object: any, options: ObjectCleanerOptions) => {
        for (const cleaner of cleaners) {
            object = cleaner(object, options)
        }
        return object
    }
}

export function makeCleanBooleanFieldsForRead(options: {
    storageRegistry: StorageRegistry
}) {
    const fieldsByCollection: { [collection: string]: string[] } = {}

    for (const [collectionName, collectionDefinition] of Object.entries(
        options.storageRegistry.collections,
    )) {
        for (const [fieldName, fieldDefinition] of Object.entries(
            collectionDefinition.fields,
        )) {
            if (fieldDefinition.type === 'boolean') {
                fieldsByCollection[collectionName] =
                    fieldsByCollection[collectionName] || []
                fieldsByCollection[collectionName].push(fieldName)
            }
        }
    }

    return function cleanBooleanFieldsForRead(
        object: any,
        options: ObjectCleanerOptions,
    ) {
        const fieldNames =
            fieldsByCollection[options.collectionDefinition.name!]
        for (const fieldName of fieldNames || []) {
            object[fieldName] = !!object[fieldName]
        }
        return object
    }
}

export function cleanOptionalFieldsForRead(
    object: any,
    options: ObjectCleanerOptions,
) {
    for (const [fieldName, fieldDefinition] of Object.entries(
        options.collectionDefinition.fields,
    )) {
        if (fieldDefinition.optional && object[fieldName] === null) {
            delete object[fieldName]
        }
    }

    return object
}

export function cleanRelationshipFieldsForWrite(
    object: any,
    options: ObjectCleanerOptions,
) {
    return _cleanRelationshipFields(
        object,
        options.collectionDefinition,
        (alias: string, fieldName: string) => {
            if (!object[alias]) {
                return
            }

            object[alias + 'Id'] = object[alias]
            delete object[alias]
        },
    )
}

export function cleanRelationshipFieldsForRead(
    object: any,
    options: ObjectCleanerOptions,
) {
    return _cleanRelationshipFields(
        object,
        options.collectionDefinition,
        (alias: string, fieldName: string) => {
            object[alias] = object[alias + 'Id']
            delete object[alias + 'Id']
        },
    )
}

export function _cleanRelationshipFields(
    object: any,
    collectionDefinition: CollectionDefinition,
    cleaner: (alias: string, fieldName: string) => void,
) {
    for (const relationship of collectionDefinition.relationships || []) {
        if (isChildOfRelationship(relationship)) {
            cleaner(relationship.alias!, relationship.fieldName!)
        } else if (isConnectsRelationship(relationship)) {
            cleaner(relationship.aliases![0], relationship.fieldNames![0])
            cleaner(relationship.aliases![1], relationship.fieldNames![1])
        }
    }

    return object
}

export function serializeJsonFields(
    object: any,
    options: ObjectCleanerOptions,
) {
    for (const [fieldName, fieldDefinition] of Object.entries(
        options.collectionDefinition.fields,
    )) {
        if (fieldDefinition.type === 'json') {
            object[fieldName] = jsonStringify(object[fieldName])
        }
    }

    return object
}

export function deserializeJsonFields(
    object: any,
    options: ObjectCleanerOptions,
) {
    for (const [fieldName, fieldDefinition] of Object.entries(
        options.collectionDefinition.fields,
    )) {
        if (fieldDefinition.type === 'json') {
            object[fieldName] =
                object[fieldName] && JSON.parse(object[fieldName])
        }
    }

    return object
}

export function supportsJsonFields(databaseType: ConnectionOptions['type']) {
    return databaseType === 'postgres'
}
