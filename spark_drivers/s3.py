import boto3


class s3(sparkBase):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def write(self):
        config = self.opts["opts"]["conf"]
        access_key = config['to']['IAM']['access_key']
        secret_access_key = config['to']['IAM']['secret_access_key']
        bc = config['to']['bucket']
        loc = f"{config['to']['folder_path']}/{config['from']['table']}.csv"

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
        )
        s3 = session.resource('s3')
        s3.meta.client.upload_file(Filename=f"out/{config['from']['table']}.csv", Bucket=bc, Key=loc)

    def read(self):
        pass

    def diagnose(self, mode):
        error_log = self.opts["opts"]["job_data"]['error_log']

        error_log.append("sqlserver diagnose not implemented")
        return {"status": False, "error_log": "s3 diagnose not implemented"}

    

