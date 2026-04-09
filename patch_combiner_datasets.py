import ijson
import json
import os
import random
import html
import unicodedata
import sys


def normalize_text(text):
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    table = str.maketrans({
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "–": "-",
        "—": "-",
        "…": "...",
    })
    return text.translate(table)


class PatchCombinerTextDatasets:
    def __init__(self, datasets_path: list, batches_per_file: int = 25000, folder_path="./datasets"):
        self.datasets_path = datasets_path
        self.batches_per_file = batches_per_file
        self.folder_path = folder_path
        self.datasets_indexs_counters = [0 for _ in range(len(datasets_path))]
        self.files_patch = list()
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def computer(self):
        for i, path in enumerate(self.datasets_path):
            with open(path, "r", encoding="utf-8") as f:
                for item in ijson.items(f, "item"):
                    self.datasets_indexs_counters[i] += 1
                    print(f"\rDataset '{path}' with batch num {self.datasets_indexs_counters[i]}", end="")
            print()

        files_num = (sum(self.datasets_indexs_counters) // self.batches_per_file)
        print(f"Tổng file patches sẽ được tạo ra là {files_num}")
        for i in range(files_num):
            self.files_patch.append(f"{self.folder_path}/patch{i+1}.json")

        for i in range(len(self.datasets_indexs_counters)):
            n = self.datasets_indexs_counters[i]
            path = self.datasets_path[i]
            writetimes_per_patch = n // (files_num-1)

            with open(path, "r", encoding="utf-8") as f:
                j = 0
                patch = list()
                if os.path.exists(self.files_patch[j]):
                    with open(self.files_patch[j], mode="r", encoding="utf-8") as file:
                        patch = json.load(file)

                for k, item in enumerate(ijson.items(f, "item")):
                    if k > ((j+1) * writetimes_per_patch):
                        random.shuffle(patch)
                        with open(self.files_patch[j], mode="w", encoding="utf-8") as file:
                            json.dump(patch, file, indent=4, ensure_ascii=False)

                        patch = list()
                        j += 1
                        if os.path.exists(self.files_patch[j]):
                            with open(self.files_patch[j], mode="r", encoding="utf-8") as file:
                                patch = json.load(file)

                    patch.append(normalize_text(item))
                    print(f"\rĐã ghi batch thứ ({k+1}/{((j+1) * writetimes_per_patch)})/{n} từ '{path}' vào path dataset '{self.files_patch[j]}'", end="")
            print()


if __name__ == "__main__":
    dataset_paths = list()
    while True:
        try:
            print("Các đường dẫn data mà bạn đã thêm.")
            if str(dataset_paths) == "[]":
                print(" + Chưa có đường dẫn nào được thêm.")
            else:
                for path in dataset_paths:
                    print(f" + {path}")
            path_inp = input("Nhập thêm đường dẫn dataset path của bạn hoặc CtrL + C để thoát.\n-> ")
            dataset_paths.append(path_inp)
            if sys.platform.startswith("win"):
                os.system("cls")
            else:
                os.system("clear")
        except KeyboardInterrupt:
            print()
            break
    batches_per_file = int(input("Nhập vào số lượng batch_size mà bạn muốn đạt được ở mỗi file patch.\n-> "))
    p = PatchCombinerTextDatasets(datasets_path=dataset_paths, batches_per_file=batches_per_file)
    p.computer()
    input("Đã hoàn thành xong việc chia file patch và combine data, Enter để thoát\n-> ")