
def apply_to_record(command, name, start, length, ffdata_in):
    with open(ffdata_in, 'rb') as ffdata:
        ffdata.seek(start)
        record = ffdata.read(length - 1)
        process = subprocess.Popen(command, stdout = subprocess.PIPE, stdin = subprocess.PIPE)
        result = process.communicate(input = record)[0]
        return (name, result)

def run_apply():

    import concurrent.futures
    import subprocess
    import os
    from sys import argv
    import shlex

    parser = argparse.ArgumentParser(description = "USAGE: ffindex_apply_py [-j JOBS] [-q] [-k] [-d DATA_FILENAME_OUT -i INDEX_FILENAME_OUT] DATA_FILENAME INDEX_FILENAME -- PROGRAM [PROGRAM_ARGS]*")

    parser.add_argument('data_filename', required = True, type = str, help = 'Input ffindex data file.')
    parser.add_argument('index_filename', required = True, type = str, help = 'Input ffindex index file.')
    parser.add_argument('command', required = True, type = str, nargs = argparse.REMAINDER, help = 'Program to be executed for every ffindex entry.')
    
    parser.add_argument('-j', type = int, default = 1, help = 'Number of parallel jobs.')
    parser.add_argument('-q', action = 'store_true', help = 'Silence the logging of every processed entry.')
    parser.add_argument('-k', action = 'store_true', help = 'Keep unmerged ffindex splits (not implemented).')
    parser.add_argument('-d', metavar = 'DATA_FILENAME_OUT', required = True, type = str, help = 'FFindex data file where the results will be saved to.')
    parser.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', required = True, type = str, help = 'FFindex index file where the results will be saved to.')
      
    args = parser.parse_args()

    ffindex_in, ffdata_in, = args.data_filename, args.index_filename
    ffindex_out, ffdata_out = args.i, args.d
    cmd = shlex.split(args.command)
    jobs = args.j
    verbose = not args.q

    # parallel execution with ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers = jobs) as executor:
        futures = []
        with open(ffindex_in, 'r') as f:
            for line in f:
                name, start, length = line.strip().split('\t') # split line using \t character
                start, length = int(start), int(length)
                futures.append(executor.submit(apply_to_record, cmd, name, start, length, ffdata_in))

        # Write the output 
        with open(ffdata_out, 'wb') as outdata, open(ffindex_out, 'w') as outindex:
            offset = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    name, result = future.result()
                except Exception as exc:
                    print(f'Record {name} generated an exception: {exc}')
                else:
                    if verbose:
                        print(name)
                    outdata.write(result + b'\0')
                    length = len(result) + 1
                    outindex.write(f"{name}\t{offset}\t{length}\n")  # use \t to join fields
                    offset += length
