import yaml
import os
class main:
    def run(self):
        self.get_source_data()
        if self.files is not None:
            if len(self.files) > 1:
                print(self.files)

    
    def get_source_data(self):
        with open('configs/settings.yaml') as setting_file:
            settings = yaml.safe_load(setting_file.read())

        self.source_data = settings
        self.files = None
        if settings['source'] in ['csv','xls','parquet'] :
            self.files = os.listdir('inputs')

            



if __name__ == '__main__':
    main_ob = main()
    main_ob.run()