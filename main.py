
from src import extract, transform, load
    
def main():
    print("Running ETL...")

    print("\tExtract step...")
    extract.extract()

    print("\tTransform step...")
    transform.transform()

    print("\tLoad step...")
    load.load()
    
if __name__ == "__main__":
    main()