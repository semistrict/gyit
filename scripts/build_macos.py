#!/usr/bin/env python3
"""Build the native Go/Swift FSKit app. Ad-hoc builds compile but may not load.

Use --identity plus --profile for an entitled development/distribution build.
The profile must authorize the extension bundle ID and FSKit capability.
"""
import argparse
import pathlib
import plistlib
import shutil
import subprocess
from macos_project import generate

ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(*args):
    subprocess.run([str(x) for x in args], cwd=ROOT, check=True)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--identity', default='-')
    p.add_argument('--profile', type=pathlib.Path)
    p.add_argument('--bundle-id', default='com.semistrict.gat')
    p.add_argument('--team', help='Use Xcode automatic signing for this Apple developer team')
    opt = p.parse_args()
    if opt.team and (opt.profile or opt.identity != '-'):
        p.error('--team cannot be combined with --identity or --profile')
    output = ROOT / '.build' / 'macos'
    output.mkdir(parents=True, exist_ok=True)
    app = output / 'gyit.app'
    extension = app / 'Contents/Extensions/gyitFS.appex'
    for bundle in (app,extension):
        (bundle/'Contents/MacOS').mkdir(parents=True, exist_ok=True)
    library=output/'libgyitfs.a'
    run('env','MACOSX_DEPLOYMENT_TARGET=26.0','CGO_CFLAGS=-mmacosx-version-min=26.0','CGO_LDFLAGS=-mmacosx-version-min=26.0','go','build','-buildmode=c-archive','-o',library,'./cmd/gyit-fskit')
    base={'CFBundleVersion':'5','CFBundleShortVersionString':'0.1','LSMinimumSystemVersion':'26.0','CFBundleDevelopmentRegion':'en',
          'CFBundleInfoDictionaryVersion':'6.0','CFBundleSupportedPlatforms':['MacOSX'],'DTPlatformName':'macosx'}
    app_info=base|{'CFBundleIdentifier':opt.bundle_id,'CFBundleName':'gyit','CFBundleExecutable':'gyit','CFBundlePackageType':'APPL'}
    attributes={
        'EXExtensionPointIdentifier':'com.apple.fskit.fsmodule',
        'FSShortName':'gyit',
        'FSActivateOptionSyntax':{'shortOptions':'g:m:o:u:'},
        'FSRequiresSecurityScopedPathURLResources':False,
        'FSSupportsPathURLs':False,'FSSupportsGenericURLResources':True,
        'FSSupportsBlockResources':False,'FSSupportsServerURLs':False,
        'FSSupportedSchemes':['https'],
        'FSPersonalities':{'gyit':{'FSName':'gyit','FSfileObjectsAreCaseSensitive':True}},
    }
    extension_info=base|{'CFBundleIdentifier':opt.bundle_id+'.filesystem','CFBundleName':'gyit filesystem','CFBundleExecutable':'gyitFS','CFBundlePackageType':'XPC!','EXAppExtensionAttributes':attributes}
    for bundle,info in ((app,app_info),(extension,extension_info)):
        (bundle/'Contents/Info.plist').write_bytes(plistlib.dumps(info))
    if opt.team:
        project = generate(ROOT, output, opt.team, opt.bundle_id, app_info, extension_info)
        run('xcodebuild', '-project', project, '-scheme', 'gyit', '-configuration', 'Debug',
            '-destination', 'platform=macOS,arch=arm64', '-derivedDataPath', output/'DerivedData',
            '-allowProvisioningUpdates', '-allowProvisioningDeviceRegistration', 'build')
        signed_app = output/'DerivedData/Build/Products/Debug/gyit.app'
        run('codesign', '--verify', '--deep', '--strict', signed_app)
        print(signed_app)
        return
    common=['xcrun','swiftc','-swift-version','5','-target','arm64-apple-macos26.0','-O']
    run(*common,'-parse-as-library','-import-objc-header',library.with_suffix('.h'),
        'macos/Volume.swift','macos/Extension.swift',library,'-framework','FSKit','-framework','CoreFoundation','-framework','Security','-lresolv','-lz',
        '-o',extension/'Contents/MacOS/gyitFS')
    run(*common,'-parse-as-library','-import-objc-header',library.with_suffix('.h'),'macos/App.swift',library,'-framework','FSKit','-framework','CoreFoundation','-framework','Security','-lresolv','-lz','-o',app/'Contents/MacOS/gyit')
    if opt.profile:
        shutil.copyfile(opt.profile,extension/'Contents/embedded.provisionprofile')
    else:
        (extension/'Contents/embedded.provisionprofile').unlink(missing_ok=True)
    run('codesign','--force','--sign',opt.identity,'--entitlements','macos/Extension.entitlements',extension)
    if opt.identity != '-':
        # Include the conventional team entitlement in manually signed builds.
        signature = subprocess.run(['codesign', '-dvv', str(extension)],
                                   capture_output=True, text=True, check=True)
        team = next((line.split('=', 1)[1] for line in signature.stderr.splitlines()
                     if line.startswith('TeamIdentifier=')), None)
        if not team or team == 'not set':
            raise RuntimeError('Signing identity has no Apple team identifier')
        entitlements = plistlib.loads((ROOT/'macos/Extension.entitlements').read_bytes())
        entitlements['com.apple.developer.team-identifier'] = team
        signed_entitlements = output/'Extension.signed.entitlements'
        signed_entitlements.write_bytes(plistlib.dumps(entitlements))
        run('codesign','--force','--sign',opt.identity,'--entitlements',signed_entitlements,extension)
    host_signing = []
    if opt.identity != '-':
        host_entitlements = output/'App.signed.entitlements'
        host_entitlements.write_bytes(plistlib.dumps({'com.apple.developer.team-identifier': team}))
        host_signing = ['--entitlements', host_entitlements]
    run('codesign','--force','--sign',opt.identity,*host_signing,app)
    run('codesign','--verify','--deep','--strict',app)
    print(app)

if __name__ == '__main__':
    main()
