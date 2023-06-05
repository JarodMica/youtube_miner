import os
import subprocess
import csv
import MeCab
import torch
import multiprocessing
import time

from multiprocessing import Process, Queue
from search import get_definition
from googletrans import Translator

playlist_link = "https://www.youtube.com/playlist?list=PLrQwEkvx5phiroDm-CGexpl8mJGrl49bo"#"https://www.youtube.com/playlist?list=PLcxHm0kSGuEAyd2SHAmb8edt3Dohd35_a"

# Directories
transcription_directory = "transcribed"
youtube_directory = "youtube_vids"
dictionary_path = "jmdict_english"

# Whisper set-up
whisper_model = "medium"
language = "ja"
if torch.cuda.is_available():
    device = "cuda:0"
else:
    device = "cpu"

def download_youtube_videos(directory, playlist_link):
    create_directory(directory)
    subprocess.run(["yt-dlp", 
                    "-x", 
                    "--audio-format", "mp3", 
                    "-o", os.path.join(directory,"%(title)s.%(ext)s"), 
                    playlist_link])

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def transcribe_videos(directory, yt_directory):
    last_skip_time = None
    while True:
        for filename in os.listdir(yt_directory):
            if filename.endswith(".mp3"):
                audio_path = os.path.join(yt_directory, filename)
                output_dir = os.path.join(directory)

                # Check if whisper transcription file already exists
                output_file = os.path.join(output_dir, os.path.splitext(filename)[0] + ".txt")
                if os.path.exists(output_file):
                    print(f"Skipping transcription for {filename}. Transcription file already exists.")
                    if last_skip_time is None:
                        last_skip_time = time.time()  # Initialize skip time
                    elif time.time() - last_skip_time > 240:  # 4 minutes * 60 seconds/minute
                        return  # Exit function if continuously skipping for more than 4 minutes
                    continue
                else:
                    last_skip_time = None  # Reset skip time when starting a new transcription

                subprocess.run(["whisper", audio_path, 
                                "--device", device,
                                "--model", whisper_model,
                                "--output_dir", output_dir,
                                "--language", language,
                                "--output_format", "txt"
                                ])

def parse_text_with_mecab(text, word_count):
    mecab = MeCab.Tagger()
    node = mecab.parse(text)

    words = []
    for line in node.split("\n"):
        if line == "EOS":
            break
        else:
            word = line.split("\t")[0]
            if word != "":
                words.append(word)

    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

def parse_transcribed_files(directory):
    word_count = {}

    for filename in os.listdir(directory):
        with open(os.path.join(directory, filename), "r", encoding="utf-8") as file:
            content = file.read()
            parse_text_with_mecab(content, word_count)

    return word_count

def translate_word(word):
    try:
        translation, hiragana_reading = get_definition(word)
        if translation == "":
            translator = Translator(service_urls=['translate.google.com'])
            translation = translator.translate(word, src='ja', dest='en').text
            hiragana_reading = ""
        return translation, hiragana_reading
    except:
        return "no defintion found"

def write_word_frequency_csv(word_count, csv_file):
    sorted_words = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    sorted_len = len(sorted_words)

    with open(csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Rank", "Word", "Hiragana", "Translation", "Frequency"])

        # Split sorting into multiple processes
        num_processes = multiprocessing.cpu_count()
        
        chunk_size = (sorted_len + num_processes - 1) // num_processes

        with multiprocessing.Pool(processes=num_processes) as pool:
            results = []
            for i in range(0, sorted_len, chunk_size):
                chunk = sorted_words[i:i+chunk_size]
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            # Wait for all processes to finish
            pool.close()
            pool.join()

            # Collect and write rows to the CSV file
            for result in results:
                rows = result.get()
                for row in rows:
                    writer.writerow(row)

def process_chunk(chunk):
    rows = []
    for rank, (word, frequency) in enumerate(chunk, 1):
        row = process_word(rank, word, frequency)
        rows.append(row)
        print(rank)
    return rows

def process_word(rank, word, frequency):
    send = translate_word(word)  # Get translation
    translation = send[0]
    hiragana_reading = send[1]
    return [rank, word, hiragana_reading, translation, frequency]

def resorter(csv_file_path):
    freq_order(csv_file_path)
    order_num(csv_file_path)

def freq_order(csv_file_path):
    with open(csv_file_path, 'r', encoding="utf-8") as file:
        reader = csv.reader(file)
        data = list(reader)

    # Sort the data based on column 4 in descending order, handling non-numeric values
    sorted_data = sorted(data[1:], key=lambda row: float(row[3]) if row[3].isnumeric() else float('inf'), reverse=True)

    with open(csv_file_path, 'w', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(data[0])  # Write the header row
        writer.writerows(sorted_data)

def order_num(csv_file_path):
    # Read the CSV file and extract the data
    with open(csv_file_path, 'r', encoding = "utf-8") as file:
        reader = csv.reader(file)
        data = list(reader)

    # Update the data with the sequential numbers
    for i in range(1, len(data)):
        data[i][0] = str(i)

    # Write the updated data back to the CSV file
    with open(csv_file_path, 'w', newline='', encoding = "utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(data)

def main():
    download_process = Process(target=download_youtube_videos, args=(youtube_directory, playlist_link))  # create a separate process to download videos
    transcribe_process = Process(target=transcribe_videos, args=(transcription_directory, youtube_directory))  # create a separate process to transcribe videos
    
    download_process.start()
    transcribe_process.start()

    download_process.join()

    # After all downloads have finished, put a sentinel value in the queue to signal the transcribe process to terminate
    # download_queue.put("DONE")
    
    transcribe_process.join()
    print("\nParsing Transcriptions:")
    word_count = parse_transcribed_files(transcription_directory)
    csv_file = "word_frequency.csv"
    csv_file_path = os.path.join(os.getcwd(), csv_file)
    print("\nOrganizing Frequency:")
    write_word_frequency_csv(word_count, csv_file_path)
    resorter(csv_file_path)

if __name__ == "__main__":
    main()
