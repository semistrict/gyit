import SwiftUI
import FSKit

@MainActor
final class MountModel: ObservableObject {
    @Published var mountPath: URL?
    @Published var message = "Mount GitHub, then open an owner/repository path."
    @Published var busy = false
    @Published var repositoryPath = "torvalds/linux"
    func mount() {
        guard #available(macOS 27.0, *) else { message = "Mounting from the app requires macOS 27."; return }
        busy = true
        FSClient.shared.mountSingleVolume(resource: FSGenericURLResource(url: URL(string:"https://github.com")!), bundleID: Bundle.main.bundleIdentifier! + ".filesystem", options:["-o","gyitcachemib=4096"]) { path,error in
            Task { @MainActor in
                self.busy = false
                if let error {
                    let detail = error.localizedDescription
                    self.message = detail == "Operation not permitted"
                        ? "Enable gyitfs in File System Extension Settings, then mount again."
                        : detail
                }
                else { self.mountPath = path; self.message = "Ready. Open a repository to start background setup." }
            }
        }
    }
    func openRepository() {
        guard let mountPath else { return }
        let parts = repositoryPath.split(separator:"/",omittingEmptySubsequences:false)
        guard parts.count >= 2, !parts.contains(where: { $0.isEmpty || $0 == "." || $0 == ".." }) else { message = "Enter owner/repository or owner/repository@revision."; return }
        let path = mountPath.appendingPathComponent("github.com").appendingPathComponent(repositoryPath)
        NSWorkspace.shared.open(path)
    }
    func unmount() {
        guard let path = mountPath else { return }; busy = true
        DispatchQueue.global().async {
            var detail: String?
            do { try NSWorkspace.shared.unmountAndEjectDevice(at:path) }
            catch { detail = error.localizedDescription }
            let ok = detail == nil
            Task { @MainActor in
                self.busy = false
                if ok { self.mountPath = nil; self.message = "Unmounted." }
                else { self.message = detail ?? "Could not unmount." }
            }
        }
    }
}
@main
struct GyitApp: App {
    @StateObject private var model = MountModel()
    var body: some Scene {
        WindowGroup("🍑gyit") {
            VStack(alignment:.leading,spacing:16) {
                Text("🍑gyit").font(.largeTitle)
                Text("GitHub, in your filesystem.")
                if let path = model.mountPath {
                    Text(path.path).font(.system(.body,design:.monospaced))
                    TextField("owner/repository@revision",text:$model.repositoryPath)
                    HStack {
                        Button("Open repository") { model.openRepository() }
                        Button("Open volume") { NSWorkspace.shared.open(path) }
                        Button("Unmount") { model.unmount() }.disabled(model.busy)
                    }
                } else {
                    Button("Mount 🍑gyit") { model.mount() }.disabled(model.busy)

                }
                if model.busy { ProgressView().controlSize(.small) }
                Text(model.message).textSelection(.enabled)
                Text("New repositories show a NOTICE with setup progress, then their complete files. Use @branch or @SHA to select a revision; encode branch slashes as %2F.").font(.caption).foregroundStyle(.secondary)
                Button("File System Extension Settings") {
                    if #available(macOS 27.0, *) { _ = FSClient.shared.openFileSystemExtensionsSettings() }
                }
            }.padding(28).frame(width:580)
        }
    }
}
