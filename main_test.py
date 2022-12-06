from concurrent.futures import ProcessPoolExecutor
import time


def main():
    with ProcessPoolExecutor(max_workers=5) as exec:
        for i in range(20):
            exec.submit(process, i)

def process(n):
    time.sleep(2)
    print(n+n)


if __name__ == "__main__":
    main()