
import os

def setup_input_output_dirs(path):
        os.makedirs(path)

if __name__ == '__main__':
    inputs_path = os.path.join('input', 'iowa-liquor-sales')
    setup_input_output_dirs(inputs_path)
    output_path = os.path.join('output')
    setup_input_output_dirs(output_path)