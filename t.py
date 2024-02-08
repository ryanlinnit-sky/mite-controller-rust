import asyncio
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
    (1, "t:j", None),
    (2, "t:j", None),
    (3, "t:j", None),
]


# Peak scenario running at full TPS for 1 hour
def s():
    for peak, journey, datapool in scenarios:
        yield journey, datapool, volume_model_factory(peak, duration=1 * 60 * 60)


# @mite_http
async def j(ctx):
    await asyncio.sleep(1)
    print("hello")
    # await ctx.http.get("http://localhost:8000/")

