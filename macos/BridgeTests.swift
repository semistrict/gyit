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
        precondition(count == 2)
        for i in 0..<Int(count) { precondition(String(cString:entries![i].name) != "NOTICE") }
        GyitFreeEntries(entries,count)
        var buffer = Data(count:4096); var n:Int32=0
        precondition(withPath(path, { GyitGeneration(v.handle,$0) }) == 2)
        precondition(v.supportedXattrNames(for:root).contains { $0.data == Data("user.gyit.control".utf8) }, "repository must advertise command discovery to FSKit")
        var endpoint: Data?
        v.getXattr(named: FSFileName(data:Data("user.gyit.control".utf8)), of: root) { data,error in
            precondition(error == nil && data != nil)
            endpoint = data
        }
        precondition(endpoint!.count > 10)
        let probe = URL(fileURLWithPath:CommandLine.arguments[2]).appendingPathComponent("discovery-test")
        try FileManager.default.createDirectory(at:probe,withIntermediateDirectories:true)
        let code = endpoint!.withUnsafeBytes { setxattr(probe.path,"user.gyit.control",$0.baseAddress,$0.count,0,0) }
        precondition(code == 0)
        let command = Process()
        command.executableURL = URL(fileURLWithPath:CommandLine.arguments[0]).deletingLastPathComponent().appendingPathComponent("gyit")
        command.currentDirectoryURL = probe
        command.arguments = ["log","--oneline","-n","1"]
        let output = Pipe(); command.standardOutput = output
        try command.run()
        let log = output.fileHandleForReading.readDataToEndOfFile()
        command.waitUntilExit()
        precondition(command.terminationStatus == 0 && String(decoding:log,as:UTF8.self).contains("fixture"))
        let readyRoot = try v.item(path)
        precondition(readyRoot.attrs.fileID == inode)
        let file = try v.item(childPath(path,Data("dir/hello".utf8)))
        precondition(file.attrs.size == 22)
        let dir = try v.item(childPath(path,Data("dir".utf8)))
        precondition(file.attrs.parentID == dir.attrs.fileID)
        try checked(buffer.withUnsafeMutableBytes { bytes in withPath(file.path) { GyitRead(v.handle,$0,6,bytes.baseAddress,5,&n) } })
        precondition(n == 5 && buffer.prefix(5) == Data("from ".utf8))
        do { _ = try v.item(childPath(path,Data("NOTICE".utf8))); fatalError("setup NOTICE survived publication") } catch { precondition((error as NSError).code == Int(ENOENT)) }
        let link = try v.item(childPath(path,Data("link".utf8)))
        v.readSymbolicLink(link) { name,error in precondition(error == nil && name?.data == Data("dir/hello".utf8)) }
        v.openItem(file,modes:.write) { error in precondition((error as NSError?)?.code == Int(EROFS)) }
        print("native synchronous-setup bridge passed")
    }
}
