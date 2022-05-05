import os
import pickle
from .server import Server

CHUNK_SIZE = 1024 * 1024 * 2

class DataServer(Server):
    def __init__(self, root_path: str):
        super().__init__(root_path=root_path)
        
        self.save_dir_file_path = os.path.join(self.root_path, 'save_dir.pkl')
        if os.path.isfile(self.save_dir_file_path):
            # load
            self.load_state()
        else:
            # init
            self.save_dir = {}
            
        self.exec = {
            'save_recv_chunks': self.save_recv_chunks,
            'output_file_chunks': self.output_file_chunks,
            'read_file': self.read_file,
            'shutdown': self.shutdown,
        }
        
    def shutdown(self):
        self.is_running = False
        self.save_state()
        
    def save_state(self):
        with open(self.save_dir_file_path, 'wb') as f:
            pickle.dump(self.save_dir, f)
            
    def load_state(self):
        with open(self.save_dir_file_path, 'rb') as f:
            self.save_dir = pickle.load(f)
        
    def save_recv_chunks(self, file):
        while True:
            (chunk, i) = self.in_chan.get()  
            # print(self, chunk[:5], i)             
            if not chunk:
                break
                
            chunk_name = '{}-part{}'.format(file, i).replace('/', '%')
            chunk_file = os.path.join(self.root_path, chunk_name)
            with open(chunk_file, 'wb') as f:
                f.write(chunk)
                
            if file not in self.save_dir.keys():
                self.save_dir[file] = [chunk_file]
            else:
                self.save_dir[file].append(chunk_file)
                
    def output_file_chunks(self, file):
        # sorted by part id
        self.save_dir[file].sort(key=lambda x: int(x.split('-')[-1][4:]))
        chunks = self.save_dir[file]
        for chunk_file in chunks:
            with open(chunk_file, 'rb') as f:
                self.out_chan.put(f.read()) # read all
            
        self.out_chan.put(None) # EOF
        
    def read_file(self, file, loc, offset):
        loc, offset = int(loc), int(offset)
        # sorted by part id
        self.save_dir[file].sort(key=lambda x: int(x.split('-')[-1][4:]))
        chunks = self.save_dir[file]
        
        chunk_id_begin = loc // CHUNK_SIZE
        chunk_id_end = (loc + offset) // CHUNK_SIZE
        for i in range(chunk_id_begin, chunk_id_end + 1):
            with open(chunks[i], 'rb') as f:
                chunk = f.read()
                if i == chunk_id_begin:
                    data = chunk[loc % CHUNK_SIZE:loc % CHUNK_SIZE + offset]
                elif i == chunk_id_end:
                    data = chunk[:(loc + offset) % CHUNK_SIZE]
                else:
                    data = chunk
                self.out_chan.put(data)
        
        self.out_chan.put(None) # EOF
   