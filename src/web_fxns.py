import re

def update_index_file(file_date):
    pattern_date = r"\d{4}-\d{2}-\d{2}"
    new_lines = []
    
    with open('index.md', 'r') as f:
        for line in f:
            re_check = re.search(pattern_date, line)
            if re_check:
                line = line.replace(re_check[0], file_date)
            new_lines.append(line)
            
    with open('index.md', 'w') as f:
        f.writelines(new_lines)