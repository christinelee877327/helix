const a = require("WAWebChatCollection")
const b = require("WAWebChatLoadMessages")
const c = require("WAWebChatGetters")
const d = require("WAWebContactCollection")
const e = require("WAWebFrontendContactGetters")
const f = require("WAWebChatGetters")
const g = require("WAWebModelStorageUtils")
const h = require("WAWebDBMessageUtils")
const l = require("WAWebSchemaMessage")
const k = require("WAWebContactCollection")
const constants = require("WAWebCollectionConstants")
const Serializer = require("WAWebDBMessageSerialization")
const EXPORT_DB_VERSION = 5
globalThis.MY_EXPORT_DB_WAS_CREATED = false
globalThis.ItsReallyOverForMe = 0
constants.PAGE_SIZE = 1000
const PAGE_SIZE = constants.PAGE_SIZE
const WRITE_CHUNK_SIZE = 500
const MAX_PAGES_PER_CHAT = 1000



//はいれつ配列
let messagePendingQueue = []
let messageProcessingQueue = []
let messageNewTimer = null
const messagePendingSet = new Set()
const recentReportedMessageKeys = new Map()
const REPORT_DEDUP_WINDOW_MS = 1500
const MESSAGE_MIN_BATCH = 10
const MESSAGE_FINALIZE_DELAY_MS = 700
let latestContactSnapshot = []
let latestChatSnapshot = []
let latestGroupSnapshot = []
let latestGroupMemberSnapshot = []
let latestHostInfoSnapshot = []

const messageListenerState = {
    installed: false,
    originalAdd: null,
    originalPut: null,
    pendingMessageTimers: new Map(),
}

const chatobj = Object.values(a?.ChatCollection?._index || {})
const chatObjIndex = Object.keys(a?.ChatCollection?._index || {})
const exportRunner = {
    running: false,
    completed: false,
    promise: null,
    waiters: [],
    // 当导出完成并打印 [dump_end] 后，可以通过此字段控制重新运行主流程的次数
    // 默认 0：不自动重跑；将其设置为 1 会在第一次结束后再跑一次（仅手动设置）
    reruns: 0,
}

const hasLostFocusState = {
    installed: false,
    value: undefined,
    pollTimer: null,
}

// function post_message(value) {
//     console.log(value)
// }


//データこうしんする
function keyToStableString(key) {
    if (key === null || key === undefined) return ''
    if (typeof key === 'string' || typeof key === 'number' || typeof key === 'boolean') return String(key)
    try {
        return JSON.stringify(key)
    } catch (err) {
        return String(key)
    }
}

function cleanupRecentReported(now = Date.now()) {
    for (const [marker, ts] of recentReportedMessageKeys) {
        if (now - ts > REPORT_DEDUP_WINDOW_MS) {
            recentReportedMessageKeys.delete(marker)
        }
    }
}

function shouldSkipRecentReported(marker, now = Date.now()) {
    const lastTs = recentReportedMessageKeys.get(marker)
    return typeof lastTs === 'number' && (now - lastTs) <= REPORT_DEDUP_WINDOW_MS
}

function markRecentlyReported(keys, now = Date.now()) {
    for (const key of keys) {
        recentReportedMessageKeys.set(keyToStableString(key), now)
    }
}

function takePendingMessageKeys() {
    const keys = messagePendingQueue
    messagePendingQueue = []
    messagePendingSet.clear()
    messageProcessingQueue = keys
    return keys
}

function enqueueMessageKey(incomingId) {
    if (!incomingId) return false
    const incomingMarker = keyToStableString(incomingId)
    const now = Date.now()
    cleanupRecentReported(now)

    if (messagePendingSet.has(incomingMarker)) return false
    if (shouldSkipRecentReported(incomingMarker, now)) return false

    messagePendingQueue.push(incomingId)
    messagePendingSet.add(incomingMarker)
    if (messageNewTimer) clearTimeout(messageNewTimer)
    messageNewTimer = setTimeout(() => {
        messageNewTimer = null
        updateMessage()
    }, 200)
    return true
}

async function seedMessageKeysByAddOnly() {
    const t_m = g.getStorage().table("message")
    if (typeof t_m.all !== 'function') {
        console.warn('t_m.allはFuncじゃない')
        return 0
    }
    const rows = await t_m.all()
    if (!Array.isArray(rows) || rows.length === 0) {
        console.log('メッセージがありません')
        return 0
    }

    const sourceKeys = []
    for (const row of rows) {
        if (row?.id) sourceKeys.push(row.id)
    }
    if (sourceKeys.length === 0) {
        console.warn('有効なsourceKeysがメッセージテーブルに見つかりません')
        return 0
    }

    const exportDb = await openExportDb()
    let added = 0
    let existed = 0
    let queued = 0

    for (let i = 0; i < sourceKeys.length; i += WRITE_CHUNK_SIZE) {
        const end = Math.min(i + WRITE_CHUNK_SIZE, sourceKeys.length)
        const tx = exportDb.transaction('messages', 'readwrite')
        const store = tx.objectStore('messages')
        const reqList = []

        for (let j = i; j < end; j++) {
            const key = sourceKeys[j]
            reqList.push(new Promise((resolve, reject) => {
                const req = store.add({ __seedOnly: true }, key)
                req.onsuccess = function () {
                    added += 1
                    if (enqueueMessageKey(key)) queued += 1
                    resolve()
                }
                req.onerror = function (event) {
                    if (req.error?.name === 'ConstraintError') {
                        if (event?.preventDefault) event.preventDefault()
                        if (event?.stopPropagation) event.stopPropagation()
                        existed += 1
                        resolve()
                        return
                    }
                    reject(req.error)
                }
            }))
        }

        await Promise.all(reqList)
        await waitTransaction(tx)
        await sleep(0)
    }

    console.log('Seed by add-only finished', {
        source: sourceKeys.length,
        added,
        existed,
        queued,
    })
    return queued
}

async function persistEntriesToExportDb(entries, chunkSize = WRITE_CHUNK_SIZE, requireBackground = false) {
    if (!Array.isArray(entries) || entries.length === 0) {
        return { added: 0, updated: 0 }
    }

    const exportDb = await openExportDb()
    let added = 0
    const duplicateKeys = []
    const duplicateValues = []

    for (let i = 0; i < entries.length; i += chunkSize) {
        if (requireBackground) {
            await waitForBackgroundMode()
        }
        const end = Math.min(i + chunkSize, entries.length)
        const tx = exportDb.transaction('messages', 'readwrite')
        const store = tx.objectStore('messages')
        const reqList = []

        for (let j = i; j < end; j++) {
            const entry = entries[j]
            reqList.push(new Promise((resolve, reject) => {
                const req = store.add(entry.value, entry.key)
                req.onsuccess = function () {
                    added += 1
                    resolve()
                }
                req.onerror = function (event) {
                    if (req.error?.name === 'ConstraintError') {
                        if (event?.preventDefault) event.preventDefault()
                        if (event?.stopPropagation) event.stopPropagation()
                        duplicateKeys.push(entry.key)
                        duplicateValues.push(entry.value)
                        resolve()
                        return
                    }
                    reject(req.error)
                }
            }))
        }

        await Promise.all(reqList)
        await waitTransaction(tx)
        await sleep(0)
    }

    let updated = 0
    if (duplicateKeys.length > 0) {
        await putKeyValueChunked(exportDb, 'messages', duplicateKeys, duplicateValues, chunkSize, false)
        updated = duplicateKeys.length
    }

    return { added, updated }
}

async function updateMessage(force = false) {
    if (updateMessage.running) return
    // if (!force && messagePendingQueue.length <= MESSAGE_MIN_BATCH) {
    //     console.log("まだデータが足りない,いまは：", messagePendingQueue.length)
    //     return
    // }
    updateMessage.running = true
    try {
        console.log("データがたまった", messagePendingQueue.length)
        const t_m = g.getStorage().table("message")

        // snapshot keys via dual-queue swap to avoid races
        const keys = takePendingMessageKeys()
        if (!Array.isArray(keys) || keys.length === 0) {
            return
        }
        console.log('処理中のkey数:', keys.length, 'sample:', keys)

        // Only use bulkGet: if unavailable or it fails, abort this update to avoid transaction issues
        const resultEntries = []
        if (typeof t_m.bulkGet !== 'function') {
            console.warn('bulkGetはFuncじゃない')
            return
        }
        try {
            const rows = await t_m.bulkGet(keys)
            console.log('bulkGet returned', rows?.length)
            for (let idx = 0; idx < rows.length; idx++) {
                const res = rows[idx]
                try { console.log('raw res snapshot idx', idx, JSON.parse(JSON.stringify(res))) } catch (e) { console.warn('failed to stringify res snapshot', e) }
                if (!res) continue
                try {
                    const converted = toJsonSafe(Serializer.messageFromDbRow(res))
                    if (converted !== undefined && converted !== null) {
                        resultEntries.push({ key: keys[idx], value: converted })
                    }
                } catch (err) {
                    console.warn('convert failed, skip', err)
                }
            }
        } catch (err) {
            console.warn('bulkGet が失敗しました。updateMessageを中止します。', err)
            return
        }

        const arr = resultEntries.map(item => item.value)
        const handledKeys = resultEntries.map(item => item.key)
        if (resultEntries.length > 0) {
            const writeStats = await persistEntriesToExportDb(resultEntries)
            console.log('もうデータはmy-export-db.messagesに書き込まれました', writeStats)
            markRecentlyReported(handledKeys)
            cleanupRecentReported()
        }

        my_log(
            arr,
            latestContactSnapshot,
            latestChatSnapshot,
            latestGroupSnapshot,
            latestGroupMemberSnapshot,
            latestHostInfoSnapshot
        )
    } finally {
        updateMessage.running = false
        messageProcessingQueue = []
        if (messagePendingQueue.length > 0 && !messageNewTimer) {
            messageNewTimer = setTimeout(() => {
                messageNewTimer = null
                updateMessage(true)
            }, 200)
        }
    }
}

//データしょうきょ
async function clearDataBase() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.deleteDatabase('my-export-db')
        req.onsuccess = function () {
            console.log('データベースは正常に削除されました')
            // reset flags so export can run again
            try {
                globalThis.MY_EXPORT_DB_WAS_CREATED = false
                exportRunner.completed = false
            } catch (e) {
                console.warn('Failed resetting exportRunner flags', e)
            }
            // schedule a re-run (avoid sync re-entry)
            setTimeout(() => {
                if (!exportRunner.running) runExportWhenBackground()
            }, 0)
            resolve()
        }
        req.onerror = function () {
            console.error('Database deletion failed', req.error)
            reject(req.error)
        }
        req.onblocked = function () {
            console.warn('deleteDatabase blocked')
        }
    })
}

//データを取る
function getData() {
    updateMessage(true)
}

function extractMessageIdFromStoreWriteArgs(args) {
    const value = args?.[0]
    const explicitKey = args?.[1]
    return value?.id ?? explicitKey
}

async function queryMessageRowById(messageId) {
    const table = g.getStorage().table("message")
    if (typeof table?.get === 'function') {
        return table.get(messageId)
    }
    if (typeof table?.bulkGet === 'function') {
        const rows = await table.bulkGet([messageId])
        return rows?.[0]
    }
    console.warn('[message-finalized] message table has no get/bulkGet')
    return undefined
}

function scheduleFinalizeMessage(incomingId, source) {
    if (!incomingId) {
        console.warn(`[message-${source}] missing id`)
        return
    }

    const marker = keyToStableString(incomingId)
    const existing = messageListenerState.pendingMessageTimers.get(marker)
    if (existing?.timer) {
        clearTimeout(existing.timer)
    }

    const timer = setTimeout(async () => {
        const pending = messageListenerState.pendingMessageTimers.get(marker)
        if (!pending) return
        messageListenerState.pendingMessageTimers.delete(marker)

        try {
            const row = await queryMessageRowById(pending.id)
            if (!row) {
                console.warn('[message-finalized] row not found', { id: pending.id })
                return
            }

            console.log('[message-finalized]', { id: pending.id, source: pending.source })
            enqueueMessageKey(pending.id)
        } catch (err) {
            console.warn('[message-finalized] query failed', { id: pending.id, err })
        }
    }, MESSAGE_FINALIZE_DELAY_MS)

    messageListenerState.pendingMessageTimers.set(marker, {
        id: incomingId,
        source,
        timer,
        ts: Date.now(),
    })
}


//监听message表变化
function listenMessageTableChanges() {
    if (messageListenerState.installed) {
        console.log('message listener already installed')
        return
    }

    if (typeof IDBObjectStore === 'undefined' || !IDBObjectStore?.prototype) {
        console.warn('IDBObjectStore is unavailable, listener not installed')
        return
    }

    post_message("MonitoringModeopen:")
    const originalAdd = IDBObjectStore.prototype.add;
    const originalPut = IDBObjectStore.prototype.put;
    messageListenerState.originalAdd = originalAdd
    messageListenerState.originalPut = originalPut

    IDBObjectStore.prototype.add = function (...args) {
        if (this.name === "message") {
            const incomingId = extractMessageIdFromStoreWriteArgs(args)
            console.log('[message-add]', { id: incomingId })
            scheduleFinalizeMessage(incomingId, 'add')
        }
        return originalAdd.apply(this, args);
    };

    IDBObjectStore.prototype.put = function (...args) {
        if (this.name === "message") {
            const incomingId = extractMessageIdFromStoreWriteArgs(args)
            console.log('[message-put]', { id: incomingId })
            scheduleFinalizeMessage(incomingId, 'put')
        }
        return originalPut.apply(this, args);
    };

    messageListenerState.installed = true
}

//1.message
//2.contact
//3.chat
//4.group
//5.groupMembers
//6.hostInfo
function my_log(message, contact, chat, group, groupMembers, hostInfo) {
    console.log("type:data", JSON.stringify({ "messages": message, "contact": contact, "chat": chat, "group": group, "groupMembers": groupMembers, "hostInfo": hostInfo }));
    console.log("かんりょう")
}

function isBackgroundMode() {
    return typeof document === 'undefined' ? true : document.hidden
}

function resolveBackgroundWaiters() {
    const waiters = exportRunner.waiters.splice(0, exportRunner.waiters.length)
    waiters.forEach(resolve => resolve())
}

function handleHasLostFocusChange(value) {
    hasLostFocusState.value = value
    if (value == 1) {
        console.log('Hasitlostfocus == 1 detected, resume export')
        resolveBackgroundWaiters()
        if (!exportRunner.running && !exportRunner.completed) {
            runExportWhenBackground()
        }
    }
}

function installHasLostFocusWatcher() {
    if (typeof globalThis === 'undefined' || hasLostFocusState.installed) return
    hasLostFocusState.installed = true

    const descriptor = Object.getOwnPropertyDescriptor(globalThis, 'Hasitlostfocus')
    if (!descriptor || descriptor.configurable) {
        const originalGetter = descriptor?.get
        const originalSetter = descriptor?.set
        let internalValue = descriptor && 'value' in descriptor ? descriptor.value : globalThis.Hasitlostfocus

        Object.defineProperty(globalThis, 'Hasitlostfocus', {
            configurable: true,
            enumerable: descriptor?.enumerable ?? true,
            get() {
                return originalGetter ? originalGetter.call(globalThis) : internalValue
            },
            set(value) {
                if (originalSetter) {
                    originalSetter.call(globalThis, value)
                } else {
                    internalValue = value
                }
                handleHasLostFocusChange(originalGetter ? originalGetter.call(globalThis) : value)
            }
        })

        handleHasLostFocusChange(originalGetter ? originalGetter.call(globalThis) : internalValue)
        return
    }

    hasLostFocusState.value = globalThis.Hasitlostfocus
    hasLostFocusState.pollTimer = setInterval(() => {
        const currentValue = globalThis.Hasitlostfocus
        if (currentValue !== hasLostFocusState.value) {
            handleHasLostFocusChange(currentValue)
        }
    }, 200)
}

function hasLostFocusOverrideEnabled() {
    return hasLostFocusState.value == 1
}

async function waitForBackgroundMode() {
    if (isBackgroundMode() || hasLostFocusOverrideEnabled()) return
    console.warn('Page is in foreground; export paused — will resume when backgrounded')
    await new Promise(resolve => exportRunner.waiters.push(resolve))
}

function requestToPromise(req) {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
    })
}

function waitTransaction(tx) {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve()
        tx.onerror = () => reject(tx.error)
        tx.onabort = () => reject(tx.error)
    })
}

function openDb(name, version = undefined) {
    return new Promise((resolve, reject) => {
        const request = version === undefined ? indexedDB.open(name) : indexedDB.open(name, version)
        let wasCreated = false
        request.onupgradeneeded = function (event) {
            wasCreated = true
            const db = event.target.result
            if (name === 'my-export-db') {
                if (!db.objectStoreNames.contains("messages")) {
                    db.createObjectStore("messages")
                }
                if (!db.objectStoreNames.contains("contact")) {
                    db.createObjectStore("contact")
                }
                if (!db.objectStoreNames.contains("chat")) {
                    db.createObjectStore("chat")
                }
                if (!db.objectStoreNames.contains("groups")) {
                    db.createObjectStore("groups")
                }
                if (!db.objectStoreNames.contains("groupMembers")) {
                    db.createObjectStore("groupMembers")
                }
                if (!db.objectStoreNames.contains("hostInfo")) {
                    db.createObjectStore("hostInfo")
                }
            }
        }
        request.onsuccess = function (event) {
            if (name === 'my-export-db') {
                globalThis.MY_EXPORT_DB_WAS_CREATED = true
            }
            resolve(event.target.result)
        }
        request.onerror = function () {
            reject(request.error)
        }
    })
}

function openExportDb() {
    return openDb('my-export-db', EXPORT_DB_VERSION)
}

async function hasExportDb() {
    if (typeof indexedDB === 'undefined') return false
    if (typeof indexedDB.databases !== 'function') {
        return !!globalThis.MY_EXPORT_DB_WAS_CREATED
    }
    try {
        const dbList = await indexedDB.databases()
        return dbList.some(item => item?.name === 'my-export-db')
    } catch (err) {
        console.warn('indexedDB.databases check failed, fallback to memory flag', err)
        return !!globalThis.MY_EXPORT_DB_WAS_CREATED
    }
}

async function clearStore(db, storeName) {
    await waitForBackgroundMode()
    const tx = db.transaction(storeName, 'readwrite')
    const store = tx.objectStore(storeName)
    await requestToPromise(store.clear())
    await waitTransaction(tx)
}

function toJsonSafe(value) {
    return JSON.parse(JSON.stringify(value, (key, currentValue) => {
        if (typeof currentValue === 'function') return undefined
        if (typeof currentValue === 'undefined') return undefined
        if (typeof currentValue === 'symbol') return undefined
        if (typeof currentValue === 'bigint') return currentValue.toString()
        return currentValue
    }))
}

function toJsonSafe_Host(value) {
    const hostExport = {
        id: value?.id?._serialized || value?.id || value?.__x_id?._serialized,
        name: value?.pushname,
        phoneNumberCreatedAt: value?.phoneNumberCreatedAt,
        phoneNumber: value?.phoneNumber
    }
    return hostExport
}

function toJsonSafe_Chat(value) {
    const chatExport = {
        id: value?.id?._serialized || value?.id || value?.__x_id?._serialized,
        name: value?.name || value?.__x_formattedTitle,
        archive: value?.archive,
        unreadCount: value?.unreadCount,
        lastReceivedKey: value?.lastReceivedKey,
        timestamp: value?.t,
        lastChatEntryTimestamp: value?.lastChatEntryTimestamp,
        isGroup: f.getIsGroup(value),
    }
    return chatExport
}

function cleanChatForExport(chat) {
    return cleanChat
}

function sanitizePage(page) {
    const result = []
    for (const item of page) {
        try {
            const safeItem = toJsonSafe(item)
            if (safeItem !== undefined) result.push(safeItem)
        } catch (err) {
            console.warn('skip one message because sanitize failed', err)
        }
    }
    return result
}

function removeChatMsgs(chat) {
    if (!chat || typeof chat !== 'object') return chat
    const clonedChat = { ...chat }
    delete clonedChat.msgs
    return clonedChat
}

function getMessageIdentity(msg) {
    if (!msg || typeof msg !== 'object') return String(msg)
    return msg?.id?._serialized
        || msg?.id?.id
        || msg?.id
        || msg?.key?.id
        || msg?.__x_id?._serialized
        || msg?.__x_id?.id
        || msg?.t
        || msg?.timestamp
        || msg?.__x_t
        || 'unknown'
}

function getPageSignature(page) {
    if (!page.length) return 'empty'
    return [
        page.length,
        getMessageIdentity(page[0]),
        getMessageIdentity(page[page.length - 1]),
    ].join(':')
}

function getStorageKey(msg, fallbackKey) {
    return msg?.id?._serialized
        || msg?.__x_id?._serialized
        || msg?.id?.id
        || msg?.key?.id
        || String(fallbackKey)
}

function sleep(ms = 0) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function getExistingStateMap(db, storeName, keys) {
    const tx = db.transaction(storeName, 'readonly')
    const store = tx.objectStore(storeName)
    const checks = keys.map(key => requestToPromise(store.get(key)).then(value => [key, value !== undefined]))
    const result = await Promise.all(checks)
    await waitTransaction(tx)
    return new Map(result)
}

async function putChunked(db, array, startKey = 0, chunkSize = WRITE_CHUNK_SIZE, stats) {
    for (let i = 0; i < array.length; i += chunkSize) {
        await waitForBackgroundMode()
        const end = Math.min(i + chunkSize, array.length)
        const keys = []
        for (let j = i; j < end; j++) {
            keys.push(getStorageKey(array[j], startKey + j))
        }
        const existingState = await getExistingStateMap(db, 'messages', keys)
        const tx = db.transaction('messages', 'readwrite')
        const store = tx.objectStore('messages')
        const promises = []
        for (let j = i; j < end; j++) {
            const key = keys[j - i]
            if (stats) {
                if (existingState.get(key)) stats.existing += 1
                else stats.inserted += 1
            }
            const req = store.put(array[j], key)
            promises.push(requestToPromise(req))
        }
        await Promise.all(promises)
        await waitTransaction(tx)
        if ((i + chunkSize) % 1000 === 0 || end === array.length) {
            console.log(`written ${end}/${array.length} in current batch`)
        }
        await sleep(0)
    }
}

async function putKeyValueChunked(db, storeName, keys, values, chunkSize = WRITE_CHUNK_SIZE, requireBackground = true) {
    for (let i = 0; i < values.length; i += chunkSize) {
        if (requireBackground) {
            await waitForBackgroundMode()
        }
        const end = Math.min(i + chunkSize, values.length)
        const tx = db.transaction(storeName, 'readwrite')
        const store = tx.objectStore(storeName)
        const promises = []
        for (let j = i; j < end; j++) {
            const req = store.put(values[j], keys[j])
            promises.push(requestToPromise(req))
        }
        await Promise.all(promises)
        await waitTransaction(tx)
        if ((i + chunkSize) % 1000 === 0 || end === values.length) {
            console.log(`written ${end}/${values.length} in ${storeName}`)
        }
        await sleep(0)
    }
}

async function readAllModelStorageRows() {
    await waitForBackgroundMode()
    // const modelDb = await openDb('model-storage')
    // const tx = modelDb.transaction('message', 'readonly')
    // const store = tx.objectStore('message')
    // const rows = await requestToPromise(store.getAll())
    const t_m = g.getStorage().table("message");
    const msgs = await t_m.all();
    // await waitTransaction(tx)
    return msgs
}

async function readAllRowsFromModelStorage(storeName) {
    await waitForBackgroundMode()
    const modelDb = await openDb('model-storage')
    const tx = modelDb.transaction(storeName, 'readonly')
    const store = tx.objectStore(storeName)
    const rows = await requestToPromise(store.getAll())
    await waitTransaction(tx)
    return rows
}

async function convertModelStorageRows(rows) {
    const result = []
    for (const row of rows) {
        try {
            const msg = Serializer.messageFromDbRow(row)
            const newmsg = toJsonSafe(msg)
            if (newmsg !== undefined) result.push(newmsg)
        } catch (err) {
            console.warn('skip one model-storage row because convert failed', err)
        }
    }
    return result
}

async function importModelStorageMessages(exportDb) {
    await waitForBackgroundMode()
    console.log('start importing from model-storage.message')
    const rows = await readAllModelStorageRows()
    console.log(`model-storage rows: ${rows.length}`)
    const messages = await convertModelStorageRows(rows)
    console.log(`converted messages: ${messages.length}`)
    const stats = { inserted: 0, existing: 0 }
    if (messages.length > 0) {
        await putChunked(exportDb, messages, 0, WRITE_CHUNK_SIZE, stats)
    }
    console.log('model-storage import finished', stats)
    // message_last = messages[messages.length - 1]
    return {
        total: messages.length,
        inserted: stats.inserted,
        existing: stats.existing,
    }
}

//消息
async function backfillMessagesFromApi(exportDb, startKey = 0) {
    await waitForBackgroundMode()
    const msgCollection = require("WAWebMsgCollection").MsgCollection
    console.warn("Starting chat messages backfill... please wait")
    let nextKey = startKey
    const stats = { inserted: 0, existing: 0, pages: 0, processed: 0 }
    for (const [chatIndex, chat] of chatobj.entries()) {
        await waitForBackgroundMode()
        if (c.getIsNewsletter(chat)) continue
        console.log('Processing chat', chatIndex, chat.id || chat)
        let pageCount = 0
        let lastPageSignature = null
        while (true) {
            try {
                await waitForBackgroundMode()
                pageCount += 1
                if (pageCount > MAX_PAGES_PER_CHAT) {
                    console.warn('reach max pages for chat, stop current chat', chat.id || chat)
                    break
                }
                let res;
                try {
                    const chatID = chat.id
                    const t = h.beginningOfChat(chatID._serialized || chatID)
                    const n = h.endOfChat(chatID._serialized || chatID)
                    const old_message = await l.getMessageTable().between(["internalId"], t, n, {
                        lowerInclusive: !1,
                        upperInclusive: !1,
                        limit: 1
                    });
                    const old_message_id = old_message[0]?.id
                    res = await b.loadEarlierMsgs({
                        "chat": chat, "msgCollection": {
                            last: () => (msgCollection.get(old_message_id)),
                            head: () => (msgCollection.get(old_message_id)),
                            length: chat.msgs.length,
                            msgLoadState: chat.msgs.msgLoadState
                        }
                    })
                } catch (e) {
                    console.error("tag:error loadEarlierMsgsError", e)
                    console.log("JS dump error")
                    return
                }

                const page = Array.isArray(res) ? res : (res ? [res] : [])
                stats.pages += 1
                const pageSignature = getPageSignature(page)
                if (pageSignature === lastPageSignature) {
                    console.warn('same page returned again, stop current chat', chat.id || chat, pageSignature)
                    break
                }
                lastPageSignature = pageSignature

                const sanitizedPage = sanitizePage(page)

                if (sanitizedPage.length > 0) {
                    console.log("Sanitizing page and importing")
                    await putChunked(exportDb, sanitizedPage, nextKey, WRITE_CHUNK_SIZE, stats)
                    nextKey += sanitizedPage.length
                    stats.processed += sanitizedPage.length
                    console.log(`saved ${sanitizedPage.length} messages, total ${nextKey}`)
                }

                if (!res || !chat.msgs.msgLoadState.noEarlierMsgs) break
                await sleep(0)
            } catch (err) {
                console.error('Error loading messages for chat', chat.id || chat, err)
                break
            }
        }
    }

    console.log('History messages stats', {
        processed: stats.processed,
        inserted: stats.inserted,
        existing: stats.existing,
        pages: stats.pages,
    })
    console.log('All writes complete, total operations:', nextKey)
    return {
        total: nextKey,
        inserted: stats.inserted,
        existing: stats.existing,
        pages: stats.pages,
        processed: stats.processed,
    }
}

//联系人
async function getContactFromApi() {
    await waitForBackgroundMode()
    console.warn("Starting contact data import... please wait")
    const contacts = d.ContactCollection.getMeContact().collection._index
    const resultKey = Object.keys(contacts)
    const result = Object.values(contacts)
    const filteredContactKeys = []
    const sanitizedContacts = []
    const phoneNumberDist = new Map()
    const contactRows = await readAllRowsFromModelStorage("contact")
    contactRows.forEach(res => {
        const key = typeof res.id === 'string' ? res.id : res.id?._serialized
        const phone = res.phoneNumber
        if (key && phone) phoneNumberDist.set(key, phone)
    })
    result.forEach((value, index) => {
        if (e.getIsMyContact(value)) {
            const contactKey = resultKey[index]
            filteredContactKeys.push(contactKey)
            const obj = toJsonSafe(value)
            const phone = phoneNumberDist.get(contactKey)
            if (phone) {
                obj.phoneNumber = phone
            }
            sanitizedContacts.push(obj)
        }
    })
    const exportDb = await openExportDb()
    await clearStore(exportDb, 'contact')
    if (sanitizedContacts.length > 0) {
        await putKeyValueChunked(exportDb, 'contact', filteredContactKeys, sanitizedContacts)
    }
    latestContactSnapshot = sanitizedContacts
    console.log('Contact data import completed, total:', sanitizedContacts.length)
    return sanitizedContacts
}

//会话
async function getChatsFromApi() {
    await waitForBackgroundMode()
    console.warn("Starting chat list import... please wait")
    const exportDb = await openExportDb()
    await clearStore(exportDb, 'chat')
    console.log(chatobj);
    const sanitizedChats = chatobj.map(value => toJsonSafe_Chat(value))
    if (sanitizedChats.length > 0) {
        await putKeyValueChunked(exportDb, 'chat', chatObjIndex, sanitizedChats)
    }
    latestChatSnapshot = sanitizedChats
    console.log('Chat list import completed, total:', sanitizedChats.length)
    return sanitizedChats
}

//群组信息
async function getGroupsFromIndexDb() {
    await waitForBackgroundMode()
    console.warn("Starting group metadata import... please wait")
    const rows = await readAllRowsFromModelStorage('group-metadata')
    const exportDb = await openExportDb()
    await clearStore(exportDb, 'groups')
    const sanitizedGroups = rows.map(row => toJsonSafe(row))
    const groupKeys = sanitizedGroups.map((row, index) => row?.id || rows[index]?.id || `group-${index}`)
    if (sanitizedGroups.length > 0) {
        await putKeyValueChunked(exportDb, 'groups', groupKeys, sanitizedGroups)
    }
    latestGroupSnapshot = sanitizedGroups
    console.log('Group metadata import completed, total:', sanitizedGroups.length)
    return sanitizedGroups
}

//群成员信息
async function getGroupMembersFromIndexDb() {
    await waitForBackgroundMode()
    console.warn("Starting group members import... please wait")
    const rows = await readAllRowsFromModelStorage('participant')
    const exportDb = await openExportDb()
    await clearStore(exportDb, 'groupMembers')
    const sanitizedMembers = rows.map(row => toJsonSafe(row))
    const memberKeys = sanitizedMembers.map((row, index) => row?.groupId || rows[index]?.groupId || `group-member-${index}`)
    if (sanitizedMembers.length > 0) {
        await putKeyValueChunked(exportDb, 'groupMembers', memberKeys, sanitizedMembers)
    }
    latestGroupMemberSnapshot = sanitizedMembers
    console.log('Group members import completed, total:', sanitizedMembers.length)
    return sanitizedMembers
}

//号主信息
async function getHostInfo() {
    await waitForBackgroundMode()
    console.warn("Starting host info import... please wait")
    try {
        const hostInfo = k.ContactCollection.getMeContact()
        const exportDb = await openExportDb()
        await clearStore(exportDb, 'hostInfo')
        const sanitizedHostInfo = toJsonSafe_Host(hostInfo)
        const hostInfoList = [sanitizedHostInfo]
        await putKeyValueChunked(exportDb, 'hostInfo', ['me'], hostInfoList, 1)
        latestHostInfoSnapshot = hostInfoList
        console.log('Host info import completed')
        return hostInfoList
    } catch (error) {
        post_message("Not logged in")
    }
}

//主函数
async function processChats() {
    console.log("Current Zodiac Sign: Aries")
    globalThis.ItsReallyOverForMe = 0
    const exportDbExists = await hasExportDb()
    try {
        const hostInfoList = await getHostInfo()
        hostInfoList?.forEach(info => { post_message("hostinfo:" + info.id) })
    } catch (error) {
        post_message("Not logged in")
    }
    globalThis.MY_EXPORT_DB_WAS_CREATED = exportDbExists
    // If DB did not exist at start, schedule exactly one rerun so
    // the second run will see the newly-created DB and enter listener mode.
    exportRunner.reruns = exportDbExists ? 0 : 1
    console.log("実行前、全部書き込みmodule", MY_EXPORT_DB_WAS_CREATED)
    if (!exportDbExists) {
        post_message("FWB:");//全量任务开始
        await waitForBackgroundMode()
        const exportDb = await openExportDb()
        await clearStore(exportDb, 'messages')
        const importedCount = await importModelStorageMessages(exportDb)
        const messageStats = await backfillMessagesFromApi(exportDb, importedCount.total)
        const contactList = await getContactFromApi()
        const chatList = await getChatsFromApi()
        const groupList = await getGroupsFromIndexDb()
        const groupMemberList = await getGroupMembersFromIndexDb()
        const hostInfoList = await getHostInfo()
        console.log('Message import summary', {
            importedTotal: importedCount.total,
            importedInserted: importedCount.inserted,
            importedExisting: importedCount.existing,
            historyProcessed: messageStats.processed,
            historyInserted: messageStats.inserted,
            historyExisting: messageStats.existing,
            historyPages: messageStats.pages,
        })
        while (globalThis.ItsReallyOverForMe === 0) {
            post_message("myexportdbdmpend:");
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    } else {
        await getContactFromApi()
        await getChatsFromApi()
        await getGroupsFromIndexDb()
        await getGroupMembersFromIndexDb()
        await getHostInfo()
        // 先补齐差异，再开启实时监听
        // add-only: 只有 add 成功(新 key)才入队并触发后续处理
        const seededCount = await seedMessageKeysByAddOnly()
        if (seededCount > 0) {
            await updateMessage(true)
        }
        //监听模式开启
        listenMessageTableChanges()
    }
}

async function runExportWhenBackground() {
    if (exportRunner.running || exportRunner.completed) return exportRunner.promise
    exportRunner.running = true
    exportRunner.promise = processChats()
        .then(() => {
            exportRunner.completed = true
            console.warn('Export process completed')
        })
        .catch(console.error)
        .finally(() => {
            exportRunner.running = false
            // 如果要求重跑一次，减少计数并重新触发（避免无限循环）
            if (exportRunner.reruns && exportRunner.reruns > 0) {
                exportRunner.reruns -= 1
                // 重置 completed 标记以允许重新运行
                exportRunner.completed = false
                // 延迟调度，避免同步嵌套导致问题
                setTimeout(() => runExportWhenBackground(), 0)
            }
        })
    return exportRunner.promise
}

installHasLostFocusWatcher()

if (typeof document !== 'undefined') {
    document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
            console.log('Page switched to background')
            resolveBackgroundWaiters()
            if (!exportRunner.running && !exportRunner.completed) {
                runExportWhenBackground()
            }
        } else {
            console.log('Page switched to foreground')
            console.warn('Will pause at current wait point or after current step')
        }
    })
}

if (isBackgroundMode()) {
    runExportWhenBackground()
} else {
    console.warn('Currently in foreground; will start export when backgrounded')
}