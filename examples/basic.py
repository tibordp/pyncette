from pyncette import Context
from pyncette import Pyncette

app = Pyncette()


@app.task(schedule="* * * * * */5")
async def hello_world(context: Context) -> None:
    print("Hello world!")


if __name__ == "__main__":
    app.main()
