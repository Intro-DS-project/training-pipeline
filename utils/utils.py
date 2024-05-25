import os
import sys
import pickle


def save_object(file_path, obj):
      try: 
         dir_path = os.path.dirname(file_path)
         os.makedirs(dir_path, exist_ok= True)
         with open(file_path, 'wb') as file_obj:
            pickle.dump(obj, file_obj)
      except Exception as e:
         print(e)


def load_object(file_path):
    try:
        with open(file_path, 'rb') as file_obj:
            return pickle.load(file_obj)
    except Exception as e:
        print(e)
        return None