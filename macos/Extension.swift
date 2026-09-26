import Foundation
import FSKit

@main
struct GyitExtension: UnaryFileSystemExtension { let fileSystem = GyitFileSystem() }
final class GyitFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {
    var volume: GyitVolume?
    func accepts(_ resource: FSResource) -> Bool {
        guard let url = (resource as? FSGenericURLResource)?.url else { return false }
        return url.scheme == "https" && url.host == "github.com" && (url.path.isEmpty || url.path == "/") && url.user == nil && url.password == nil && url.query == nil && url.fragment == nil
    }
    func probeResource(resource: FSResource, replyHandler: @escaping (FSProbeResult?, Error?) -> Void) {
        guard accepts(resource) else { return replyHandler(.notRecognized,nil) }
        replyHandler(.usable(name:"gyit",containerID:FSContainerIdentifier(uuid:UUID())),nil)
    }
    func loadResource(resource: FSResource, options: FSTaskOptions, replyHandler: @escaping (FSVolume?, Error?) -> Void) {
        guard volume == nil else { return replyHandler(nil,posix(EBUSY)) }
        guard accepts(resource) else { return replyHandler(nil,posix(EINVAL)) }
        do {
            var mib: Int64 = 4096
            for option in options.taskOptions { for field in option.split(separator:",") {
                if field.hasPrefix("gyitcachemib=") { mib = Int64(field.dropFirst("gyitcachemib=".count)) ?? -1 }
            } }
            guard mib >= 0 && mib <= 1 << 30 else { throw posix(EINVAL) }
            let data = FileManager.default.urls(for:.applicationSupportDirectory,in:.userDomainMask)[0].appendingPathComponent("gyit/repositories").path
            let cache = FileManager.default.urls(for:.cachesDirectory,in:.userDomainMask)[0].appendingPathComponent("gyit/decoded-v1").path
            let v = try GyitVolume(data:data,cache:cache,budget:mib << 20)
            volume = v; containerStatus = .ready; replyHandler(v,nil)
        } catch { replyHandler(nil,error) }
    }
    func unloadResource(resource: FSResource, options: FSTaskOptions, replyHandler: @escaping (Error?) -> Void) { volume = nil; replyHandler(nil) }
}
