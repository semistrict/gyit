import Foundation
import FSKit

@main
struct BridgeTests {
    static func main() throws {
        guard CommandLine.arguments.count == 5 else { fatalError("usage: test REMOTE DATA CACHE SHA") }
        let v = try GyitVolume(data:CommandLine.arguments[2],cache:CommandLine.arguments[3],remote:CommandLine.arguments[1],budget:16<<20)
        precondition(v.root.attrs.mode == 0o555 && v.root.attrs.parentID == .parentOfRoot)
        let path = Data("github.com/acme/project@\(CommandLine.arguments[4])".utf8)
        let root = try v.item(path)
        let inode = root.attrs.fileID
        var entries:UnsafeMutablePointer<GyitEntry>?; var count:Int32=0
        try checked(withPath(path) { p in withPath(Data()) { GyitList(v.handle,p,$0,&entries,&count) } })
        precondition(count == 1 && String(cString:entries![0].name) == "NOTICE")
        GyitFreeEntries(entries,count)
        let notice = try v.item(childPath(path,Data("NOTICE".utf8)))
        var buffer = Data(count:4096); var n:Int32=0
        try checked(buffer.withUnsafeMutableBytes { bytes in withPath(notice.path) { GyitRead(v.handle,$0,0,bytes.baseAddress,4096,&n) } })
        precondition(String(decoding:buffer.prefix(Int(n)),as:UTF8.self).contains("preparing"))
        let deadline = Date().addingTimeInterval(20)
        while withPath(path, { GyitGeneration(v.handle,$0) }) == 1 && Date() < deadline { Thread.sleep(forTimeInterval:0.02) }
        precondition(withPath(path, { GyitGeneration(v.handle,$0) }) == 2)
        let readyRoot = try v.item(path)
        precondition(readyRoot.attrs.fileID == inode)
        let file = try v.item(childPath(path,Data("dir/hello".utf8)))
        precondition(file.attrs.size == 22)
        let dir = try v.item(childPath(path,Data("dir".utf8)))
        precondition(file.attrs.parentID == dir.attrs.fileID)
        try checked(buffer.withUnsafeMutableBytes { bytes in withPath(file.path) { GyitRead(v.handle,$0,6,bytes.baseAddress,5,&n) } })
        precondition(n == 5 && buffer.prefix(5) == Data("from ".utf8))
        do { _ = try v.item(notice.path); fatalError("setup NOTICE survived publication") } catch { precondition((error as NSError).code == Int(ENOENT)) }
        let link = try v.item(childPath(path,Data("link".utf8)))
        v.readSymbolicLink(link) { name,error in precondition(error == nil && name?.data == Data("dir/hello".utf8)) }
        v.openItem(file,modes:.write) { error in precondition((error as NSError?)?.code == Int(EROFS)) }
        print("native background-setup bridge passed")
    }
}
