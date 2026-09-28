#!/usr/bin/env python3
"""Compile and exercise native Swift callbacks and the real Go bridge on a small repo."""
import os
import argparse
import pathlib
import subprocess
import tempfile

ROOT=pathlib.Path(__file__).resolve().parents[1]
BUILD=ROOT/'.build/macos'

def run(args,**kwargs):
    return subprocess.run([str(x) for x in args],check=True,**kwargs)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mount',type=pathlib.Path,help='Check Finder metadata on an already mounted volume instead of bridge tests')
    parser.add_argument('--path',action='append',default=[],help='Relative child path to check with --mount')
    args=parser.parse_args()
    if args.mount:
        BUILD.mkdir(parents=True,exist_ok=True)
        run(['xcrun','clang','-Wno-deprecated-declarations',ROOT/'macos/MountedTests.c','-framework','CoreServices','-o',BUILD/'mounted-tests'])
        run([BUILD/'mounted-tests',args.mount,*[args.mount/path for path in args.path]],timeout=30)
        return
    run(['python3',ROOT/'scripts/build_macos.py'],cwd=ROOT)
    run(['go','build','-o',BUILD/'gyit','./cmd/gyit'],cwd=ROOT)
    run(['xcrun','swiftc','-swift-version','5','-target','arm64-apple-macos26.0','-O','-parse-as-library','-import-objc-header',BUILD/'libgyitfs.h',ROOT/'macos/Volume.swift',ROOT/'macos/BridgeTests.swift',BUILD/'libgyitfs.a','-framework','FSKit','-framework','CoreFoundation','-framework','Security','-lresolv','-lz','-o',BUILD/'bridge-tests'])
    with tempfile.TemporaryDirectory(prefix='gyit-native-tests-') as tmp:
        tmp=pathlib.Path(tmp);source=tmp/'source';source.mkdir()
        env=os.environ|{'GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_NOSYSTEM':'1','GIT_AUTHOR_NAME':'Test','GIT_AUTHOR_EMAIL':'test@example.test','GIT_COMMITTER_NAME':'Test','GIT_COMMITTER_EMAIL':'test@example.test'}
        def git(*args):return run(['git','-C',source,*args],env=env,capture_output=True).stdout.decode().strip()
        git('init','-q','-b','main');(source/'dir').mkdir();(source/'dir/hello').write_text('hello from a snapshot\n');(source/'link').symlink_to('dir/hello')
        git('add','.');git('commit','-qm','fixture');sha=git('rev-parse','HEAD')
        git('checkout','-qb','next');(source/'dir/hello').write_text('updated content has a different length\n');(source/'added').write_text('new file\n');(source/'link').unlink();(source/'link').symlink_to('added')
        git('add','.');git('commit','-qm','next');git('checkout','-q','main')
        remotes=tmp/'remotes/acme';remotes.mkdir(parents=True)
        git('clone','--bare','--quiet',str(source),str(remotes/'project.git'))
        run(['git','-C',remotes/'project.git','config','uploadpack.allowFilter','true'])
        run(['git','-C',remotes/'project.git','config','uploadpack.allowAnySHA1InWant','true'])
        run([BUILD/'bridge-tests',(tmp/'remotes').as_uri(),tmp/'data',tmp/'cache',sha],timeout=30)

if __name__=='__main__':main()
