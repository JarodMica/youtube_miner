import os
import json
import glob
import pickle

dictionary = "jmdict_english"

def load_cache(cache_file):
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
    else:
        cache = {'definitions': {}, 'hiragana_readings': {}}

    return cache

def save_cache(cache, cache_file):
    with open(cache_file, 'wb') as f:
        pickle.dump(cache, f)

def build_cache(directory, cache_file):
    cache = load_cache(cache_file)
    files = glob.glob(os.path.join(directory, '*.json'))
    for file in files:
        # Check filename before processing
        filename = os.path.basename(file)
        if filename in ["index.json", "tag_bank_1.json"]:
            continue
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for term_data in data:
            term = term_data[0]
            definition = term_data[5]
            hiragana_reading = term_data[1]  # Extract the hiragana reading

            if term in cache['definitions']:
                cache['definitions'][term].append(definition)
            else:
                cache['definitions'][term] = [definition]
            
            cache['hiragana_readings'][term] = hiragana_reading

    save_cache(cache, cache_file)
    
def get_definition(term, max_def = 3, directory=dictionary, cache_file='cache.pkl'):
    cache = load_cache(cache_file)

    if not cache['definitions']:
        build_cache(directory, cache_file)
        cache = load_cache(cache_file)

    definitions = cache['definitions'].get(term, [])
    hiragana_reading = cache['hiragana_readings'].get(term, None)
    top_3 = get_top3(definitions, max_def)

    return top_3, hiragana_reading

def get_top3(definitions, max_def):
    # Convert each definition list to a string, then get the first three
    first_three_definitions = [', '.join(definition) for definition in definitions[:max_def]]

    # Join the first three definitions into a single string
    first_three_definitions_str = '; '.join(first_three_definitions)
    return first_three_definitions_str

def main():
    word = "彼"
    definitions, hiragana_reading = get_definition(word)
    print(definitions)
    print(hiragana_reading)
    # print(type(hiragana_reading))
    # print(type(definitions))

if __name__ == "__main__":
    main()
