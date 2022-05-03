from pathlib import Path

class ChartImage:
    def __init__(self, storage, workload):
        self.path = Path('.' , 'images' , storage , workload)

    def parented_path(self, prefetcher):
        self.path.mkdir(parents=True, exist_ok=True)
        self.path.chmod(0o777)
        self.path = self.path.joinpath(prefetcher + '.png')
        return self.path
