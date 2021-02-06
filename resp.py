import json
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Resp():
    def __init__(self):
        self.success_template = {"code": 200, "data": None}
        self.fail_template = {"code": 400, "msg": "operation fail"}

    def success(self, data=None):
        self.success_template["data"] = data
        json_data = json.dumps(self.success_template)
        self.success_template["data"] = None
        return json_data

    def fail(self, msg="operation fail"):
        self.fail_template["msg"] = msg
        json_data = json.dumps(self.fail_template)
        self.fail_template["msg"] = "operation fail"
        return json_data


if __name__ == '__main__':
    resp = Resp()
    logger.info("info: %()", resp.success())
    print(resp.success())
    print(resp.fail())
    print(resp.success("data1"))
    print(resp.fail("msg1"))
    print(resp.success())
    print(resp.fail())