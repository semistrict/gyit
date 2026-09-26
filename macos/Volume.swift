import Foundation
import FSKit
import Darwin

func posix(_ code: Int32) -> Error { NSError(domain: NSPOSIXErrorDomain, code: Int(code)) }
func checked(_ code: Int32) throws { if code != 0 { throw posix(code) } }
func withPath<T>(_ path: Data, _ body: (UnsafeMutablePointer<CChar>) throws -> T) rethrows -> T {
    var terminated = path + Data([0])
    return try terminated.withUnsafeMutableBytes { try body($0.baseAddress!.assumingMemoryBound(to: CChar.self)) }
}
func childPath(_ parent: Data, _ name: Data) -> Data { parent.isEmpty ? name : parent + Data([47]) + name }

final class GyitItem: FSItem {
    let path: Data
    private var storedEntry: GyitEntry
    private let entryLock = NSLock()
    var entry: GyitEntry {
        get { entryLock.lock(); defer { entryLock.unlock() }; return storedEntry }
        set { entryLock.lock(); storedEntry = newValue; entryLock.unlock() }
    }
    // Only a bounded continuation window is retained. Older cookies can restart
    // scanning; directory contents are immutable for this volume's lifetime.
    var cursors: [UInt64: Data] = [0: Data()]
    let cursorLock = NSLock()
    init(path: Data, entry: GyitEntry) { self.path = path; self.storedEntry = entry; super.init() }
    var kind: FSItem.ItemType {
        switch entry.mode & 0o170000 { case 0o040000, 0o160000: return .directory; case 0o120000: return .symlink; default: return .file }
    }
    var attrs: FSItem.Attributes {
        let a = FSItem.Attributes()
        a.type = kind; a.mode = entry.mode & 0o555
        // Git tree/gitlink modes carry no permission bits; directories must
        // still be readable and traversable in the mounted filesystem.
        if kind == .directory { a.mode = 0o555 }
        if kind == .symlink { a.mode = 0o777 }
        a.size = UInt64(max(0, entry.size)); a.allocSize = a.size
        a.fileID = path.isEmpty ? .rootDirectory : FSItem.Identifier(entry.inode)
        if path.isEmpty { a.parentID = .parentOfRoot }
        else if let slash = path.lastIndex(of: 47) {
            a.parentID = FSItem.Identifier(withPath(Data(path[..<slash])) { GyitInode($0) })
        } else { a.parentID = .rootDirectory }
        a.flags = 0
        // Git trees contain no per-file timestamps. Report a deterministic
        // epoch rather than leaving standard attributes unsupported.
        let epoch = timespec(tv_sec: 0, tv_nsec: 0)
        a.modifyTime = epoch; a.changeTime = epoch; a.accessTime = epoch
        a.birthTime = epoch; a.addedTime = epoch; a.backupTime = epoch
        a.linkCount = kind == .directory ? 2 : 1
        a.uid = getuid(); a.gid = getgid()
        a.supportsLimitedXAttrs = true
        return a
    }
}

final class GyitVolume: FSVolume, FSVolume.Operations, FSVolume.ReadWriteOperations, FSVolume.XattrOperations, FSVolume.OpenCloseOperations {
    let handle: UInt64
    let root: GyitItem
    let itemLock = NSLock()
    var items: [Data: GyitItem] = [:]
    init(data: String, cache: String, remote: String = "https://github.com", token: String = "", budget: Int64 = 4 << 30) throws {
        var h: UInt64 = 0
        let code = data.withCString { data in cache.withCString { cache in remote.withCString { remote in token.withCString { token in
            GyitOpen(UnsafeMutablePointer(mutating: data), UnsafeMutablePointer(mutating: cache), UnsafeMutablePointer(mutating: remote), UnsafeMutablePointer(mutating: token), budget, &h)
        } } } }
        try checked(code); handle = h
        root = GyitItem(path: Data(), entry: GyitEntry(inode: 2, size: 0, mode: 0o040000, name: nil))
        super.init(volumeID: FSVolume.Identifier(uuid: UUID()), volumeName: FSFileName(string: "gyit"))
        items[Data()] = root
    }
    deinit { GyitClose(handle) }
    func route(_ path: Data) throws -> (UInt64, Data) { (handle, path) }
    func item(_ path: Data) throws -> GyitItem {
        itemLock.lock(); defer { itemLock.unlock() }
        if path.isEmpty { return root }
        if let item = items[path] {
            var fresh = GyitEntry()
            try checked(withPath(path) { GyitLookup(handle, $0, &fresh) })
            item.entry = fresh
            return item
        }
        let (h, relative) = try route(path)
        var e = GyitEntry(); try checked(withPath(relative) { GyitLookup(h, $0, &e) })
        e.inode = withPath(path) { GyitInode($0) }
        let item = GyitItem(path: path, entry: e); items[path] = item; return item
    }
    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let c = FSVolume.SupportedCapabilities(); c.supportsSymbolicLinks = true
        c.supports64BitObjectIDs = true
        // Stable inode numbers do not imply support for lookup by file ID.
        c.supportsPersistentObjectIDs = false
        c.caseFormat = .sensitive; c.doesNotSupportSettingFilePermissions = true
        return c
    }
    var volumeStatistics: FSStatFSResult { let s = FSStatFSResult(fileSystemTypeName: "gyit"); s.blockSize = 4096; s.ioSize = 1 << 20; return s }
    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var restrictsOwnershipChanges: Bool { true }
    var truncatesLongNames: Bool { false }
    var maximumXattrSize: Int { 4096 }
    var maximumFileSize: UInt64 { UInt64(Int64.max) }
    func activate(options: FSTaskOptions, replyHandler: @escaping (FSItem?, Error?) -> Void) { replyHandler(root, nil) }
    func deactivate(options: FSDeactivateOptions, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }
    func mount(options: FSTaskOptions, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }
    func unmount(replyHandler: @escaping () -> Void) { replyHandler() }
    func synchronize(flags: FSSyncFlags, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }
    func reclaimItem(_ item: FSItem, replyHandler: @escaping (Error?) -> Void) {
        itemLock.lock()
        let remove = { if let node = item as? GyitItem, !node.path.isEmpty, self.items[node.path] === node { self.items.removeValue(forKey: node.path) } }
        if #available(macOS 27.0, *) { _ = item.tryReclaim(remove) } else { remove() }
        itemLock.unlock(); replyHandler(nil)
    }
    func lookupItem(named name: FSFileName, inDirectory directory: FSItem, replyHandler: @escaping (FSItem?, FSFileName?, Error?) -> Void) {
        guard let dir = directory as? GyitItem, !name.data.isEmpty, !name.data.contains(47), !name.data.contains(0) else { return replyHandler(nil,nil,posix(EINVAL)) }
        do { replyHandler(try item(childPath(dir.path,name.data)),name,nil) } catch { replyHandler(nil,nil,error) }
    }
    func getAttributes(_ request: FSItem.GetAttributesRequest, of item: FSItem, replyHandler: @escaping (FSItem.Attributes?, Error?) -> Void) {
        guard let item = item as? GyitItem else { return replyHandler(nil,posix(EINVAL)) }
        do { replyHandler(try self.item(item.path).attrs,nil) } catch { replyHandler(nil,error) }
    }
    func enumerateDirectory(_ directory: FSItem, startingAt cookie: FSDirectoryCookie, verifier: FSDirectoryVerifier, attributes: FSItem.GetAttributesRequest?, packer: FSDirectoryEntryPacker, replyHandler: @escaping (FSDirectoryVerifier, Error?) -> Void) {
        guard let dir = directory as? GyitItem else { return replyHandler(verifier,posix(EINVAL)) }
        dir.cursorLock.lock(); defer { dir.cursorLock.unlock() }
        let generation = withPath(dir.path) { GyitGeneration(handle, $0) }
        if verifier.rawValue != 0 && verifier.rawValue != generation { dir.cursors = [0:Data()] }
        let currentVerifier = FSDirectoryVerifier(rawValue:generation)
        var ordinal = verifier.rawValue != 0 && verifier.rawValue != generation ? 0 : UInt64(cookie.rawValue)
        let bias: UInt64 = attributes == nil ? 2 : 0
        if attributes == nil {
            if ordinal == 0 {
                if !packer.packEntry(name:FSFileName(string:"."),itemType:.directory,itemID:dir.attrs.fileID,nextCookie:FSDirectoryCookie(rawValue:1),attributes:nil) { return replyHandler(currentVerifier,nil) }
                ordinal = 1
            }
            if ordinal == 1 {
                let parentPath = dir.path.lastIndex(of:47).map { Data(dir.path[..<$0]) } ?? Data()
                do {
                    let parent = try item(parentPath)
                    if !packer.packEntry(name:FSFileName(string:".."),itemType:.directory,itemID:parent.attrs.fileID,nextCookie:FSDirectoryCookie(rawValue:2),attributes:nil) { return replyHandler(currentVerifier,nil) }
                } catch { return replyHandler(verifier,error) }
                ordinal = 2
            }
            ordinal -= 2
        }
        var after = dir.cursors[ordinal] ?? Data()
        var scanned: UInt64 = dir.cursors[ordinal] == nil ? 0 : ordinal
        do {
            while true {
                var entries: UnsafeMutablePointer<GyitEntry>?; var count: Int32 = 0
                let (h, relative) = try route(dir.path)
                try checked(withPath(relative) { path in withPath(after) { GyitList(h,path,$0,&entries,&count) } })
                guard let entries else { if scanned < ordinal { throw posix(EINVAL) }; break }
                defer { GyitFreeEntries(entries,count) }
                var full = false
                for i in 0..<Int(count) {
                    var entry = entries[i]
                    let name = Data(bytes: entry.name!, count: strlen(entry.name!))
                    if scanned < ordinal { scanned += 1; after = name; continue }
                    let path = childPath(dir.path,name)
                    entry.inode = withPath(path) { GyitInode($0) }
                    let child = GyitItem(path: path,entry:entry)
                    let next = ordinal + 1
                    if !packer.packEntry(name: FSFileName(data:name), itemType:child.kind, itemID:child.attrs.fileID, nextCookie:FSDirectoryCookie(rawValue:next+bias), attributes:attributes == nil ? nil : child.attrs) { full = true; break }
                    ordinal = next; scanned = next; after = name
                    dir.cursors[ordinal] = after
                    if dir.cursors.count > 256 { dir.cursors = [ordinal:after,0:Data()] }
                }
                if full || count < 128 { break }
            }
            replyHandler(currentVerifier,nil)
        } catch { replyHandler(verifier,error) }
    }
    func read(from item: FSItem, at offset: off_t, length: Int, into buffer: FSMutableFileDataBuffer, replyHandler: @escaping (Int, Error?) -> Void) {
        guard let item = item as? GyitItem, length >= 0 else { return replyHandler(0,posix(EINVAL)) }
        var count: Int32 = 0
        guard let (h, relative) = try? route(item.path) else { return replyHandler(0,posix(EISDIR)) }
        let code = buffer.withUnsafeMutableBytes { buffer in withPath(relative) { GyitRead(h,$0,offset,buffer.baseAddress,Int32(min(length,buffer.count,8<<20)),&count) } }
        replyHandler(Int(count),code == 0 ? nil : posix(code))
    }
    func readSymbolicLink(_ item: FSItem, replyHandler: @escaping (FSFileName?, Error?) -> Void) {
        guard let item = item as? GyitItem, item.entry.size >= 0, item.entry.size <= 1<<20 else { return replyHandler(nil,posix(EINVAL)) }
        var data = Data(count:Int(item.entry.size)); var n: Int32 = 0
        guard let (h, relative) = try? route(item.path) else { return replyHandler(nil,posix(EINVAL)) }
        let code = data.withUnsafeMutableBytes { bytes in withPath(relative) { GyitRead(h,$0,0,bytes.baseAddress,Int32(bytes.count),&n) } }
        if code != 0 { return replyHandler(nil,posix(code)) }; replyHandler(FSFileName(data:data.prefix(Int(n))),nil)
    }
    func write(contents: Data, to item: FSItem, at offset: off_t, replyHandler: @escaping (Int, Error?) -> Void) { replyHandler(0,posix(EROFS)) }
    func setAttributes(_ attributes: FSItem.SetAttributesRequest, on item: FSItem, replyHandler: @escaping (FSItem.Attributes?, Error?) -> Void) {
        guard let node = item as? GyitItem else { return replyHandler(nil,posix(EINVAL)) }
        let times: FSItem.Attribute = [.accessTime,.modifyTime,.changeTime]
        for bit in 0..<18 {
            let field = FSItem.Attribute(rawValue: 1 << bit)
            if attributes.isValid(field) && !times.contains(field) { return replyHandler(nil,posix(EROFS)) }
        }
        guard attributes.isValid(.accessTime) || attributes.isValid(.modifyTime) else { return replyHandler(nil,posix(EROFS)) }
        let code = withPath(node.path) { GyitRetry(handle,$0) }
        guard code == 0 else { return replyHandler(nil,posix(code)) }
        attributes.consumedAttributes = times
        replyHandler(node.attrs,nil)
    }
    func createItem(named name: FSFileName, type: FSItem.ItemType, inDirectory directory: FSItem, attributes: FSItem.SetAttributesRequest, replyHandler: @escaping (FSItem?, FSFileName?, Error?) -> Void) { replyHandler(nil,nil,posix(EROFS)) }
    func createSymbolicLink(named name: FSFileName, inDirectory directory: FSItem, attributes: FSItem.SetAttributesRequest, linkContents: FSFileName, replyHandler: @escaping (FSItem?, FSFileName?, Error?) -> Void) { replyHandler(nil,nil,posix(EROFS)) }
    func createLink(to item: FSItem, named name: FSFileName, inDirectory directory: FSItem, replyHandler: @escaping (FSFileName?, Error?) -> Void) { replyHandler(nil,posix(EROFS)) }
    func renameItem(_ item: FSItem, inDirectory source: FSItem, named name: FSFileName, to newName: FSFileName, inDirectory target: FSItem, overItem: FSItem?, replyHandler: @escaping (FSFileName?, Error?) -> Void) { replyHandler(nil,posix(EROFS)) }
    func removeItem(_ item: FSItem, named name: FSFileName, fromDirectory directory: FSItem, replyHandler: @escaping (Error?) -> Void) { replyHandler(posix(EROFS)) }
    func supportedXattrNames(for item: FSItem) -> [FSFileName] {
        guard let item = item as? GyitItem,
              let path = String(data:item.path,encoding:.utf8),
              path.split(separator:"/",omittingEmptySubsequences:false).count == 3,
              path.hasPrefix("github.com/") else { return [] }
        return [FSFileName(data:Data("user.gyit.control".utf8))]
    }
    func getXattr(named name: FSFileName, of item: FSItem, replyHandler: @escaping (Data?, Error?) -> Void) {
        guard name.data == Data("user.gyit.control".utf8), let item = item as? GyitItem else { replyHandler(nil,posix(ENOATTR)); return }
        var bytes: UnsafeMutablePointer<CChar>?; var count: Int32 = 0
        let code = withPath(item.path) { GyitEndpoint(handle,$0,&bytes,&count) }
        guard code == 0, let bytes else { replyHandler(nil,posix(code)); return }
        defer { free(bytes) }
        replyHandler(Data(bytes:bytes,count:Int(count)),nil)
    }
    func setXattr(named name: FSFileName, to data: Data?, on item: FSItem, policy: FSVolume.SetXattrPolicy, replyHandler: @escaping (Error?) -> Void) { replyHandler(posix(EROFS)) }
    func listXattrs(of item: FSItem, replyHandler: @escaping ([FSFileName]?, Error?) -> Void) { replyHandler(supportedXattrNames(for:item),nil) }

    func openItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler: @escaping (Error?) -> Void) { replyHandler(modes.contains(.write) ? posix(EROFS) : nil) }
    func closeItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }

}

@available(macOS 27.0, *)
extension GyitVolume: FSVolume.DataCacheHandler {
    func open(_ item: FSItem, modes: FSVolume.OpenModes, cacheMode: FSVolume.DataCacheMode, context: FSContext, replyHandler: @escaping (FSOpenItemResult?, Error?) -> Void) {
        guard !modes.contains(.write) else { return replyHandler(nil,posix(EROFS)) }
        replyHandler(FSOpenItemResult(grantedCoherency: .noCache),nil)
    }
    func close(_ item: FSItem, context: FSContext, replyHandler: @escaping () -> Void) { replyHandler() }
    func upgrade(_ item: FSItem, cacheMode: FSVolume.DataCacheMode, context: FSContext, replyHandler: @escaping (FSUpgradeItemResult?, Error?) -> Void) {
        replyHandler(FSUpgradeItemResult(grantedCoherency: .noCache),nil)
    }
}
