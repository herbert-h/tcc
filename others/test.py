from functools import lru_cache

@lru_cache(maxsize=256)
def f(x):
    return x

def main():
    y = []
    f(5)
    f(10)
    print(y)


if __name__ == "__main__":
    main()