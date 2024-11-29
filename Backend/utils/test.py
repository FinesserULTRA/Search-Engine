# from tokenizer import Tokenizer
import pandas as pd
from file_io import *

if __name__ == "__main__":
    # tk = Tokenizer()
    lex = read_json('../index data/lexicon_tokenize.json')
    
    all_tokens = []
    for token in lex:
        # print(f"{token} appended")
        # all_tokens.append(tk.correct_spelling(token))
        continue

    list(set(all_tokens))
    
    # print(all_tokens)