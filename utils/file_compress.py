import os
import subprocess
import gzip


def compress(filenames,compression='gzip'):
	"""Compresses files to .lzo and returns their names"""

	if compression.lower() == 'gzip':
		filename_extension = '.gz'
	elif compression.lower() == 'lzop':
		filename_extension = '.lzo'
	
	if isinstance(filenames, str):
		filenames = [filenames]

	for file in filenames:
		"""If an already compressed filename is passed, skip it"""
		if not file.endswith(filename_extension):
			input_filepath = os.path.join('data/',file)
			output_filepath = os.path.join('data/',file+filename_extension)
			if compression.lower() == 'lzop':
				subprocess.run(['lzop', '-o',output_filepath,input_filepath])	
			elif compression.lower() == 'gzip':
				with open(input_filepath, 'rb') as input_file:
					with gzip.open(output_filepath, 'wb+') as output_file:
						output_file.writelines(input_file)


	return [filename+filename_extension if not filename.endswith(filename_extension) else filename for filename in filenames]