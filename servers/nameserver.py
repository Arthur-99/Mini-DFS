       
import os
import pickle

from colorama import Fore

from .server import Server
from .dataserver import DataServer
from typing import List


class NameServer(Server):
    """
    Management of files and dataservers.
    """
    def __init__(self, root_path: str, dataservers: List[DataServer]):
        super().__init__(root_path=root_path)
        self.dataservers = dataservers
        
        self.file_tree_path = os.path.join(self.root_path, 'file_tree.pkl')
        if os.path.isfile(self.file_tree_path):
            # load
            with open(self.file_tree_path, 'rb') as f:
                self.file_tree = pickle.load(f)
        else:
            # init
            self.file_tree = {'.files': set()}
            
        self.exec = {
            'upload': self.upload,
            'download': self.download,
            'read': self.read,
            'shutdown': self.shutdown,
        }
        
    def shutdown(self):
        self.is_running = False
        with open(self.file_tree_path, 'wb') as f:
            pickle.dump(self.file_tree, f)
        
            
    def upload(self, file: str, dir: str):
        curr = self.to_dir(dir)
        curr['.files'].add(file)
        
        # send cmd to dataserver threads
        for ds in self.dataservers:
            ds.cmd_chan.put(' '.join(['save_recv_chunks', os.path.join(dir, file)]))
            
        while True:
            (chunk, i) = self.in_chan.get()   
            # send to dataservers
            for ds in self.dataservers:
                ds.in_chan.put((chunk, i))
            if not chunk:
                break
            
    def download(self, file_path):
        assert self.exists(file_path)
        # choose one ds
        ds = self.dataservers[0]
        ds.cmd_chan.put(' '.join(['output_file_chunks', file_path]))
        while True:
            # send to client
            chunk = ds.out_chan.get()  
            self.out_chan.put(chunk) 
            if not chunk:
                break
    
    def read(self, file_path, loc, offset):
        assert self.exists(file_path)
        # choose one ds
        ds = self.dataservers[0]
        ds.cmd_chan.put(' '.join(['read_file', file_path, loc, offset]))
        while True:
            # send to client
            chunk = ds.out_chan.get()  
            self.out_chan.put(chunk) 
            if not chunk:
                break
        
    def mkdir(self, dir):
        curr_dir = self.file_tree
        dirs = [d for d in dir.split('/') if d != '']
        for d in dirs:
            if d not in curr_dir.keys():
                curr_dir[d] = {'.files': set()}
            curr_dir = curr_dir[d]
            
    def deldir(self, dir):
        dirs = [d for d in dir.split('/') if d != '']
        last_dir = '/'.join(dirs[:-1])
        del self.to_dir(last_dir)[dirs[-1]]
        
    def to_dir(self, dir):
        curr_dir = self.file_tree
        dirs = [d for d in dir.split('/') if d != '']
        for d in dirs:
            curr_dir = curr_dir[d]
        return curr_dir

    def ls(self, dir=''):
        curr = self.to_dir(dir)
        for k in curr.keys():
            if k == '.files':
                # file
                for f in curr[k]:
                    print(f, end=' ' * 2)
            else:
                # folder
                print(Fore.BLUE + k + Fore.RESET, end=' ' * 2)
        print()
        
    def exists(self, file_path):
        dirs = [d for d in file_path.split('/') if d != '']
        dir = '/'.join(dirs[:-1])
        file = dirs[-1]
        curr_dir = self.to_dir(dir)
        return file in curr_dir['.files']
    