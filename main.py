import json
import sqlite3
from typing import List, Union
from count_lines import rawgencount


def main():
    filename = "words.json"
    number_of_lines = rawgencount(filename=filename)

    connection = sqlite3.connect("data.db")
    cursor = connection.cursor()
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS Dictionary (
                word_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                pos TEXT NOT NULL,
                word TEXT NOT NULL,
                sounds TEXT
            )
        """
    )
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS Senses (
                sense_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                word_id INTEGER NOT NULL,
                glosses TEXT NOT NULL,
                example TEXT,
                FOREIGN KEY (word_id)
                    REFERENCES Dictionary (word_id)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
            )
        """
    )

    line_number = 0
    with open("words.json", encoding="utf-8") as f:
        for line in f:
            line_number += 1

            data = json.loads(line)

            if "source" in data and data["source"] == "thesaurus":
                continue

            parsed_data = [
                data["pos"] if "pos" in data else None,
                data["word"] if "word" in data else "",
            ]
            if "sounds" in data:
                for sound in data["sounds"]:
                    if "ipa" in sound:
                        parsed_data.append(sound["ipa"])
                        break
                else:
                    parsed_data.append(None)
            else:
                parsed_data.append(None)
            print(round((line_number/number_of_lines) * 100, 2), parsed_data)

            cursor.execute(
                "INSERT INTO Dictionary (pos, word, sounds) VALUES (?, ?, ?)", parsed_data)
            senses: List[List[Union[str, int, None]]] = []
            for sense in data["senses"]:
                parsed_sense: List[Union[str, int, None]] = []
                if "glosses" in sense and cursor.lastrowid:
                    parsed_sense = [
                        cursor.lastrowid,
                        sense["glosses"][0],
                    ]
                if "examples" in sense:
                    if "type" in sense["examples"][0] and sense["examples"][0]["type"] == "example":
                        parsed_sense.append(sense["examples"][0]["text"])
                    else: parsed_sense.append(None)
                else: parsed_sense.append(None)
                senses.append(parsed_sense)
            try:
                cursor.executemany(
                    "INSERT INTO Senses (word_id, glosses, example) VALUES (?, ?, ?)", senses)
            except Exception as e:
                print(f"EXCEPTION: {e}")

    connection.commit()
    cursor.execute("VACUUM")
    
    connection.close()
    


if __name__ == "__main__":
    main()
