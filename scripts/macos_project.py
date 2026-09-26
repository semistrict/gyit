"""Generate an Xcode project for automatic FSKit signing (output is ignored)."""
import hashlib
import pathlib
import plistlib


def generate(root, output, team, bundle_id, app_info, extension_info):
    objects = {}

    def add(key_name, isa, **fields):
        key = hashlib.sha256(key_name.encode()).hexdigest()[:24].upper()
        objects[key] = dict(isa=isa, **fields)
        return key

    def configurations(name, settings):
        config = add(name + '-debug', 'XCBuildConfiguration', name='Debug', buildSettings=settings)
        return add(name + '-configs', 'XCConfigurationList', buildConfigurations=[config],
                   defaultConfigurationIsVisible=0, defaultConfigurationName='Debug')

    def source(name):
        ref = add(name, 'PBXFileReference', lastKnownFileType='sourcecode.swift',
                  path=str(root/'macos'/name), sourceTree='<absolute>')
        return ref, add(name + '-build', 'PBXBuildFile', fileRef=ref)

    app_ref, app_build = source('App.swift')
    volume_ref, volume_build = source('Volume.swift')
    ext_ref, ext_build = source('Extension.swift')
    icon_ref = add('app-icon', 'PBXFileReference', lastKnownFileType='image.icns',
                   path=str(output/'gyit.icns'), sourceTree='<absolute>')
    icon_build = add('app-icon-build', 'PBXBuildFile', fileRef=icon_ref)
    app_product = add('app-product', 'PBXFileReference', explicitFileType='wrapper.application',
                      path='gyit.app', sourceTree='BUILT_PRODUCTS_DIR')
    ext_product = add('ext-product', 'PBXFileReference', explicitFileType='wrapper.extensionkit-extension',
                      path='gyitfs.appex', sourceTree='BUILT_PRODUCTS_DIR')
    products = add('products', 'PBXGroup', name='Products', sourceTree='<group>',
                   children=[app_product, ext_product])
    group = add('main-group', 'PBXGroup', sourceTree='<group>',
                children=[app_ref, volume_ref, ext_ref, icon_ref, products])
    for name, info in [('App', app_info), ('Extension', extension_info)]:
        (output/(name + '.Info.plist')).write_bytes(plistlib.dumps(info))
    shared = dict(ARCHS='arm64', SDKROOT='macosx', MACOSX_DEPLOYMENT_TARGET='26.0',
                  SWIFT_VERSION='5.0', CODE_SIGN_STYLE='Automatic', DEVELOPMENT_TEAM=team,
                  CODE_SIGN_IDENTITY='Apple Development', ENABLE_HARDENED_RUNTIME='YES', GENERATE_INFOPLIST_FILE='NO',
                  ENABLE_USER_SCRIPT_SANDBOXING='YES', ALWAYS_SEARCH_USER_PATHS='NO')
    ext_settings = shared | dict(
        PRODUCT_NAME='gyitfs', PRODUCT_BUNDLE_IDENTIFIER=bundle_id + '.filesystem',
        INFOPLIST_FILE=str(output/'Extension.Info.plist'),
        CODE_SIGN_ENTITLEMENTS=str(root/'macos/Extension.entitlements'),
        SWIFT_OBJC_BRIDGING_HEADER=str(output/'libgyitfs.h'),
        OTHER_LDFLAGS=['$(inherited)', str(output/'libgyitfs.a'), '-framework', 'FSKit',
                      '-framework', 'CoreFoundation', '-framework', 'Security', '-lresolv', '-lz'],
        ENABLE_APP_SANDBOX='YES', SKIP_INSTALL='YES')
    ext_sources = add('ext-sources', 'PBXSourcesBuildPhase', buildActionMask=2147483647,
                      files=[volume_build, ext_build], runOnlyForDeploymentPostprocessing=0)
    ext_target = add('ext-target', 'PBXNativeTarget', name='gyitfs', productName='gyitfs',
                     productReference=ext_product, productType='com.apple.product-type.extensionkit-extension',
                     buildConfigurationList=configurations('ext', ext_settings),
                     buildPhases=[ext_sources], buildRules=[], dependencies=[])
    dependency = add('ext-dependency', 'PBXTargetDependency', target=ext_target)
    embedded = add('embedded-extension', 'PBXBuildFile', fileRef=ext_product,
                   settings={'ATTRIBUTES': ['RemoveHeadersOnCopy']})
    embed_phase = add('embed-extensions', 'PBXCopyFilesBuildPhase', name='Embed Extensions',
                      buildActionMask=2147483647, dstPath='$(CONTENTS_FOLDER_PATH)/Extensions',
                      dstSubfolderSpec=16, files=[embedded], runOnlyForDeploymentPostprocessing=0)
    app_sources = add('app-sources', 'PBXSourcesBuildPhase', buildActionMask=2147483647,
                      files=[app_build], runOnlyForDeploymentPostprocessing=0)
    app_resources = add('app-resources', 'PBXResourcesBuildPhase', buildActionMask=2147483647,
                        files=[icon_build], runOnlyForDeploymentPostprocessing=0)
    app_settings = shared | dict(PRODUCT_NAME='gyit', PRODUCT_BUNDLE_IDENTIFIER=bundle_id,
                                 INFOPLIST_FILE=str(output/'App.Info.plist'),
                                 CODE_SIGN_ENTITLEMENTS=str(root/'macos/App.entitlements'),
                                 ENABLE_APP_SANDBOX='YES',
                                 SWIFT_OBJC_BRIDGING_HEADER=str(output/'libgyitfs.h'),
                                 OTHER_LDFLAGS=ext_settings['OTHER_LDFLAGS'])
    app_target = add('app-target', 'PBXNativeTarget', name='gyit', productName='gyit',
                     productReference=app_product, productType='com.apple.product-type.application',
                     buildConfigurationList=configurations('app', app_settings),
                     buildPhases=[app_sources, app_resources, embed_phase], buildRules=[], dependencies=[dependency])
    project = add('project', 'PBXProject', compatibilityVersion='Xcode 14.0',
                  buildConfigurationList=configurations('project', {}),
                  mainGroup=group, productRefGroup=products, projectDirPath='', projectRoot='',
                  targets=[app_target, ext_target], knownRegions=['en', 'Base'], developmentRegion='en',
                  attributes={'TargetAttributes': {
                      app_target: {'ProvisioningStyle': 'Automatic', 'DevelopmentTeam': team},
                      ext_target: {'ProvisioningStyle': 'Automatic', 'DevelopmentTeam': team}}})
    path = output/'gyit.xcodeproj'
    path.mkdir(exist_ok=True)
    (path/'project.pbxproj').write_bytes(plistlib.dumps(dict(
        archiveVersion='1', objectVersion='56', classes={}, objects=objects, rootObject=project)))
    return path
