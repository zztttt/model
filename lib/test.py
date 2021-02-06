# def execute(model):
#     str = "execute:" + model
#     return str

def execute(*args):
    for arg in args:
        print("arg in *args:", arg)
    return "execute finish"