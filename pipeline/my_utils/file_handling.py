import argparse

def prepend_to_file(path,line):
    """
    Adds the line to the beginning of the provided path.
    """
    with open(path,'r+') as f:
        line.rstrip()
        output = [line]
        output.append(f.read())
        f.seek(0,0)
        f.write("\n".join(output))
if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('file', type=str, help='The file we are working with')
    parser.add_argument('--prepend', dest="prepend", type=str, default=None, help='Prepends the given quoted string to the given file.')

    args = parser.parse_args()
    if args.prepend:
        prepend_to_file(args.file,args.prepend)
