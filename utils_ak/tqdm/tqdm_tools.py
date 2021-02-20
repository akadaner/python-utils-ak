""" Tqdm wrapper with more functionality. """
import time
from datetime import datetime
from tqdm import tqdm as tqdm_vanilla
from tqdm import tqdm_notebook

BACKEND = "tqdm"


try:
    from IPython import get_ipython

    if "IPKernelApp" in get_ipython().config:
        from tqdm import tqdm_notebook as tqdm_backend

        BACKEND = "tqdm_notebook"
except:
    pass


class tqdm:
    def __init__(
        self,
        *args,
        desc=None,
        postfix=None,
        show_timing=False,
        timing_format="%H:%M:%S",
        hook=None,
        **kwargs
    ):
        tqdm_backend = tqdm_notebook if BACKEND == "tqdm_notebook" else tqdm_vanilla
        self.backend = tqdm_backend(*args, **kwargs)
        self.desc = desc
        self.postfix = postfix
        self.hook = hook
        self.show_timing = show_timing
        self.timing_format = timing_format
        self.start = datetime.now()

    def __iter__(self):
        for obj in self.backend.__iter__():
            if self.desc:
                if isinstance(self.desc, str):
                    desc = self.desc
                else:
                    desc = str(self.desc(obj))
            else:
                desc = ""
            postfix_dict = {}
            if self.show_timing:
                timing_str = "[{} - {}] ".format(
                    self.format_dt(self.start), self.format_dt(datetime.now())
                )
                if BACKEND == "tqdm_notebook":
                    postfix_dict["clock"] = timing_str
                else:
                    desc = timing_str + desc
            if desc:
                self.backend.set_description(desc)

            if self.postfix:
                postfix_dict.update(self.postfix(obj))

            if postfix_dict:
                self.backend.set_postfix(postfix_dict)

            if self.hook:
                self.hook(obj)

            yield obj

    def format_dt(self, dt):
        return dt.strftime(self.timing_format)


def test():
    def sample_hook(i):
        if i % 100 == 0:
            print("\nThis is a hook message", i)

    for x in tqdm(
        range(100),
        show_timing=True,
        desc=lambda i: i ** 2,
        postfix=lambda i: {"i": i // 10},
        hook=sample_hook,
    ):
        time.sleep(0.01)


if __name__ == "__main__":
    test()
