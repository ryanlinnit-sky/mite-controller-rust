# from mite_http import mite_http

# from mite.scenario import StopVolumeModel


def volume_model_factory(n, duration=60 * 5):
    def vm(start, end):
        if start > duration:
            raise Exception("Volume model is done")
            # raise StopScenario
        return n

    vm.__name__ = f"volume model {n}"
    return vm


scenarios = [
    (10, "t:j", None),
    (20, "t:j", None),
    (30, "t:j", None),
]


# Peak scenario running at full TPS for 1 hour
def s():
    for peak, journey, datapool in scenarios:
        yield journey, datapool, volume_model_factory(peak, duration=1 * 60 * 60)


# @mite_http
async def j(ctx):
    print("hello")
    # await ctx.http.get("http://localhost:8000/")

