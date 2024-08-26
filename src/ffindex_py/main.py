
import concurrent.futures
import subprocess
import sys
import argparse
import contextlib

version = "1.0"
visit = "Visit the project at https://github.com/alephreish/ffindex-py"


@contextlib.contextmanager
def open_file_or_stdout(filename, mode = 'w'):
    fh = sys.stdout if filename == '-' else open(filename, mode)
    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()

def read_fasta(file):
    seq = name = header = ''
    for line in file:
        if line.startswith('>'):
            if seq:
                yield name, header, seq
            seq = ''
            header = line[1:].rstrip()
            name = header.split()[0]
        else:
            seq += line
    if seq:
        yield name, header, seq

def read_ffindex(file):
    for index, line in enumerate(file):
        name, start, length = line.split('\t')
        yield index, name, int(start), int(length)

def apply_to_record(command, name, start, length, ffdata_in):
    with open(ffdata_in, 'rb') as ffdata:
        ffdata.seek(start)
        record = ffdata.read(length - 1)
        process = subprocess.Popen(command, stdout = subprocess.PIPE, stdin = subprocess.PIPE, stderr = subprocess.PIPE)
        stdout, stderr = process.communicate(input = record)
        return name, stdout, stderr, process.returncode

def run_get():
    description = "ffindex_get re-implementation in python"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-n', action = 'store_true', help = 'Use index of entry instead of entry name.')
    arg_group.add_argument('--entries-file', metavar = 'FILE_WITH_ENTRIES', type = str, help = 'Text file with entries (or indices).')

    arg_group.add_argument('-d', metavar = 'DATA_FILENAME_OUT', required = True, type = str, help = 'FFindex data file where the results will be saved to.')
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', required = True, type = str, help = 'FFindex index file where the results will be saved to.')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')
    arg_group.add_argument('entries', metavar = 'entry name(s)', type = str, nargs = argparse.REMAINDER, help = 'Entry names (or indices).')

    args = parser.parse_args()
    use_index = args.n
    ffdata_in = args.data_filename
    ffindex_in = args.index_filename
    ffdata_out = args.d
    ffindex_out = args.i

    assert args.entries_file or args.entries and not (args.entries_file and args.entries), "Either specify in-line entries or a file with entries"

    entries = []
    if args.entries_file:
        with open(args.entries_file) as file:
            entries = [ line.rstrip() for line in file ]
    else:
        entries = args.entries
    assert len(entries) == len(set(entries)), "The list of the entries has duplicates"

    if use_index:
        for i, entry in enumerate(entries):
            try:
                entries[i] = int(entry)
            except Exception as e:
                raise Exception(f"Integer expected for entry index, got '{entry}'")

    found = [ (None, None, None) ] * len(entries)

    with open(ffindex_in, 'r') as index_in, open(ffdata_in, 'rb') as data_in, open(ffindex_out, 'w') as index_out, open(ffdata_out, 'wb') as data_out:
        offset = 0
        for index, name, start, length in read_ffindex(index_in):
            needle = index if use_index else name
            if needle in entries:
                i = entries.index(needle)
                data_in.seek(start)
                record = data_in.read(length)
                data_out.write(record)
                found[i] = name, offset, length
                offset += length
        for i, (name, offset, length) in enumerate(found):
            assert name is not None, f"Requested entry '{entries[i]}' not found in the index"
            index_out.write(f"{name}\t{offset}\t{length}\n")

def run_rename():
    description = "rename ffindex records"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', required = True, type = str, help = 'FFindex index file where the results will be saved to.')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')

    args = parser.parse_args()
    ffdata_in = args.data_filename
    ffindex_in = args.index_filename
    ffindex_out = args.i if args.i else '-'

    names = {}
    with open(ffindex_in) as index_in, open(ffdata_in) as data_in, open_file_or_stdout(ffindex_out, 'w') as index_out:
        for index, name, start, length in read_ffindex(index_in):
            data_in.seek(start)
            data_line = next(data_in)
            new_name = data_line.lstrip('#>').split(maxsplit = 1)[0]
            assert new_name, f"Empty name at index {index}"
            assert new_name not in names, f"Duplicate name '{new_name}'"
            names[new_name] = True
            index_out.write(f'{new_name}\t{start}\t{length}\n')

def run_apply():

    class CustomHelpFormatter(argparse.HelpFormatter):
        def __init__(self, prog):
            super().__init__(prog, max_help_position = 50)

    description = "ffindex_apply re-implementation in python"
    description += "\nNote that the ffindex records are sorted by default (as in ffindex_apply_mpi).\nTo keep the input order (as in ffindex_apply) use --keep-order"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-j', metavar = "JOBS", type = int, default = 1, help = 'Number of parallel jobs.')
    arg_group.add_argument('-q', action = 'store_true', help = 'Silence the logging of every processed entry.')
    arg_group.add_argument('-k', action = 'store_true', help = 'Keep unmerged ffindex splits (not implemented).')
    arg_group.add_argument('--keep-order', action = 'store_true', help = 'Keep ffindex record order (important: this argument is absent in ffindex_apply and ffindex_apply_mpi).')
    arg_group.add_argument('-d', metavar = 'DATA_FILENAME_OUT', required = True, type = str, help = 'FFindex data file where the results will be saved to.')
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', required = True, type = str, help = 'FFindex index file where the results will be saved to.')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')
    arg_group.add_argument('command', metavar = 'PROGRAM [PROGRAM_ARGS]*', type = str, nargs = '+', help = 'Program to be executed for every ffindex entry.')

    usage_str = parser.format_usage() # get the generated usage string
  
    # make changes to the usage_str as desired
    usage_str = usage_str.replace("usage: ", "")
    usage_str = usage_str.replace("...", "-- PROGRAM [PROGRAM_ARGS]*")
    parser.usage = usage_str

    args = parser.parse_args()

    ffdata_in,  ffindex_in  = args.data_filename, args.index_filename
    ffdata_out, ffindex_out = args.d, args.i
    cmd = args.command
    jobs = args.j
    verbose = not args.q
    sort_order = not args.keep_order

    # parallel execution with ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers = jobs) as executor:
        futures = []
        ffindex = {}
        names = []
        with open(ffindex_in, 'r') as f:
            for index, name, start, length in read_ffindex(f):
                ffindex[name] = start, length
                names.append(name)
        if sort_order:
            names.sort()
        for name in names:
            start, length = ffindex[name]
            futures.append(executor.submit(apply_to_record, cmd, name, start, length, ffdata_in))

        # Write the output
        with open(ffdata_out, 'wb') as outdata, open(ffindex_out, 'w') as outindex:
            offset = 0
            index_buf = {}
            for future in concurrent.futures.as_completed(futures):
                try:
                    name, stdout, stderr, returncode = future.result()
                    if returncode > 0:
                        message = stderr.decode("utf-8")
                        raise Exception(f"Got exit code {returncode} with stderr content: {message}")
                except Exception as e:
                    print(f'Record {name} generated an exception: {e}')
                else:
                    if verbose:
                        print(name)
                    outdata.write(stdout + b'\0')
                    length = len(stdout) + 1
                    index_buf[name] = offset, length
                    offset += length
                    # to guarantee the order of ffindex
                    while names and names[0] in index_buf:
                        name0 = names.pop(0)
                        offset0, length0 = index_buf.pop(name0)
                        outindex.write(f"{name0}\t{offset0}\t{length0}\n")

def run_reindex():
    description = "Re-index an existing .ffdata file"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default=argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('ffdata', metavar = 'DATA_FILENAME_IN', type = str, help = 'Path to the ffdata file to be reindexed')
    arg_group.add_argument('ffindex', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'Path to the output ffindex file')

    args = parser.parse_args()

    ffdata_file = args.ffdata
    ffindex_file = args.ffindex

    with open(ffdata_file, 'rb') as ffdata_file, open(ffindex_file, 'w') as ffindex_file:
        chunk_size = 1024*1024  # chunk size of 1MB
        offset = 0
        record_length = 0
        name = 0
        while True:
            chunk = ffdata_file.read(chunk_size)
            if chunk:  # if data exists in chunk
                for byte in chunk:
                    if byte == 0:  # if null character is found
                        ffindex_file.write(f'{name}\t{offset}\t{record_length+1}\n')
                        offset += record_length + 1
                        record_length = 0
                        name += 1
                    else:
                        record_length += 1
            else:
                # End of file, checking if there's a record without trailing null character
                if record_length > 0:
                    ffindex_file.write(f'{name}\t{offset}\t{record_length+1}\n')
                break

def run_from_fasta():
    description = "Create a ffindex database from a fasta file"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default=argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('ffdata', metavar = 'DATA_FILENAME_OUT', type = str, help = 'Path to output ffdata file.')
    arg_group.add_argument('ffindex', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'Path to output ffindex file.')
    arg_group.add_argument('fasta', metavar = 'FASTA', type = str, help = 'Path to input fasta file.')

    args = parser.parse_args()

    fasta_file = args.fasta
    ffdata_file, ffindex_file = args.ffdata, args.ffindex

    with open(fasta_file, 'r') as fasta, open(ffdata_file, 'wb') as ffdata, open(ffindex_file, 'w') as ffindex:
        offset = 0
        for name, header, seq in read_fasta(fasta):
            fasta_record_bytes = f'>{header}\n{seq}\0'.encode('utf-8')
            record_length = len(fasta_record_bytes)
            ffdata.write(fasta_record_bytes)
            ffindex.write(f'{name}\t{offset}\t{record_length}\n')
            offset += record_length
