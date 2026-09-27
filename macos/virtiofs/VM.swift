import Foundation
import Virtualization

// The delegate copies each request once before Go parses it; guest memory is
// never inspected twice. One queue notification drains all available work.
final class FilesystemDevice: NSObject, VZCustomVirtioDeviceConfigurationDelegate, VZCustomVirtioDeviceDelegate {
    let handle: UInt64
    let deviceQueue=DispatchQueue(label:"gyit.virtio.device")
    let workers=DispatchQueue(label:"gyit.virtio.workers",attributes:.concurrent)
    // Queue ownership stays on deviceQueue. Only private buffers cross to Go.
    // At most four requests per device can allocate input/output buffers.
    var pending:[UInt16:VZVirtioQueue]=[:]
    var active=0
    var failed=false
    init(data:String,cache:String,remote:String,checkout:String,ttl:Int32) throws {
        handle = data.withCString { d in cache.withCString { c in remote.withCString { r in checkout.withCString { p in
            GyitVirtioOpen(UnsafeMutablePointer(mutating:d),UnsafeMutablePointer(mutating:c),UnsafeMutablePointer(mutating:r),UnsafeMutablePointer(mutating:p),ttl)
        } } } }
        if handle == 0 { throw NSError(domain:"gyit",code:1,userInfo:[NSLocalizedDescriptionKey:"cannot open filesystem backend"]) }
    }
    deinit { GyitVirtioClose(handle) }
    func configuration(tag:String) -> VZCustomVirtioDeviceConfiguration {
        let config=VZCustomVirtioDeviceConfiguration()
        config.deviceID=26 // VIRTIO_ID_FS
        config.pciClassID=1;config.pciSubclassID=0x80
        config.virtioQueueCount=2 // high-priority queue and one request queue
        var bytes=Data(tag.utf8);bytes.append(Data(count:36-bytes.count))
        bytes.append(contentsOf:[1,0,0,0]) // num_request_queues, little endian
        config.deviceSpecificConfiguration=VZVirtioDeviceSpecificConfiguration(configurationData:bytes)
        config.provider=VZCustomVirtioDeviceDelegateProvider(deviceQueue:deviceQueue,delegate:self)
        return config
    }
    func customVirtioConfiguration(_ configuration:VZCustomVirtioDeviceConfiguration,didCreateDevice device:VZCustomVirtioDevice) {
        device.delegate=self
    }
    func customVirtioDevice(_ device:VZCustomVirtioDevice,didReceiveNotificationFor queue:VZVirtioQueue) {
        pending[queue.queueIndex]=queue
        drain(device)
    }
    func drain(_ device:VZCustomVirtioDevice) {
        while active<4 && !failed {
            var next:VZVirtioQueueElement?
            for index in pending.keys.sorted() {
                if let element=pending[index]?.nextElement() {next=element;break}
                pending.removeValue(forKey:index)
            }
            guard let element=next else {return}
            do {
                let size=element.readBuffersAvailableByteCount
                let capacity=element.writeBuffersAvailableByteCount
                guard size>=40 && size<=8<<20 && capacity<=8<<20 else {throw NSError(domain:"gyit",code:2)}
                let snapshot=try element.readBytes(withExactLength:size)
                active+=1
                workers.async {
                    var input=snapshot
                    var output=Data(count:capacity)
                    let count=input.withUnsafeMutableBytes { i in output.withUnsafeMutableBytes { o in
                        GyitVirtioRequest(self.handle,i.baseAddress,Int32(size),o.baseAddress,Int32(capacity))
                    } }
                    let reply=output
                    self.deviceQueue.async {
                        do {
                            guard count>=0 && count<=capacity else {throw NSError(domain:"gyit",code:3)}
                            if count>0 {try element.write(reply.prefix(Int(count)))}
                        } catch {
                            fputs("virtio-fs device failed: \(error)\n",stderr)
                            self.failed=true;device.requestReset()
                        }
                        element.returnToQueue()
                        self.active-=1
                        self.drain(device)
                    }
                }
            } catch {
                fputs("virtio-fs device failed: \(error)\n",stderr)
                element.returnToQueue();failed=true;device.requestReset();return
            }
        }
    }
}

final class MachineDelegate:NSObject,VZVirtualMachineDelegate {
    func guestDidStop(_ vm:VZVirtualMachine) {GyitVirtioFinishProfile();exit(0)}
    func virtualMachine(_ vm:VZVirtualMachine,didStopWithError error:Error) {fputs("VM stopped: \(error)\n",stderr);exit(1)}
}

// Intentionally a standalone test harness; no changes to the installed app.
// KERNEL INITRD DISK CHECKOUT DATA CACHE REMOTE TTL_SECONDS [custom|apple]
let args=CommandLine.arguments
guard (args.count==9 || args.count==10), let ttl=Int32(args[8]), ttl>=0, ttl<=86400 else {
    fputs("usage: gyit-vm KERNEL INITRD DISK CHECKOUT DATA CACHE REMOTE TTL_SECONDS (0..86400) [custom|apple]\n",stderr);exit(2)
}
let baseline=args.count==10 ? args[9] : "custom"
guard baseline=="custom" || baseline=="apple" else {
    fputs("checkout baseline must be custom or apple\n",stderr);exit(2)
}
var devices=[try FilesystemDevice(data:args[5],cache:args[6],remote:args[7],checkout:"",ttl:ttl)]
let config=VZVirtualMachineConfiguration()
config.cpuCount=4;config.memorySize=8<<30
let boot=VZLinuxBootLoader(kernelURL:URL(fileURLWithPath:args[1]))
boot.initialRamdiskURL=URL(fileURLWithPath:args[2])
boot.commandLine="root=LABEL=cloudimg-rootfs rw console=hvc0 init=/bin/bash quiet"
config.bootLoader=boot
let disk=VZVirtioBlockDeviceConfiguration(attachment:try VZDiskImageStorageDeviceAttachment(url:URL(fileURLWithPath:args[3]),readOnly:false))
config.storageDevices=[disk]
let serial=VZVirtioConsoleDeviceSerialPortConfiguration()
serial.attachment=VZFileHandleSerialPortAttachment(fileHandleForReading:.standardInput,fileHandleForWriting:.standardOutput)
config.serialPorts=[serial]
config.entropyDevices=[VZVirtioEntropyDeviceConfiguration()]
config.customVirtioDevices=[devices[0].configuration(tag:"gyit")]
if baseline=="apple" {
    let checkout=VZVirtioFileSystemDeviceConfiguration(tag:"checkout")
    checkout.share=VZSingleDirectoryShare(directory:VZSharedDirectory(url:URL(fileURLWithPath:args[4]),readOnly:true))
    config.directorySharingDevices=[checkout]
} else {
    devices.append(try FilesystemDevice(data:"",cache:"",remote:"",checkout:args[4],ttl:ttl))
    config.customVirtioDevices.append(devices[1].configuration(tag:"checkout"))
}
print("checkout baseline: \(baseline)")
try config.validate()
let machine=VZVirtualMachine(configuration:config)
let delegate=MachineDelegate();machine.delegate=delegate
machine.start { result in if case .failure(let error)=result {fputs("VM start failed: \(error)\n",stderr);exit(1)} }
RunLoop.main.run()
