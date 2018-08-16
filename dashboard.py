import psycopg2
import os
from datetime import datetime

def get_params():
    params = {
        "db_host": os.environ.get('DB_HOST'),
        "db_port": int(os.environ.get('DB_PORT')),
        "db_name": os.environ.get('DB_NAME'),
        "db_user": os.environ.get('DB_USER'),
        "db_pass": os.environ.get('DB_PASS'),
        "data_dir": os.environ.get('DATA_DIR')
    }
    return params

class ReportGenerator():
    def __init__(self):
        self.params = get_params()
        self.conn = psycopg2.connect(host=self.params['db_host'], port=self.params['db_port'], dbname=self.params['db_name'], user=self.params['db_user'], password=self.params['db_pass'])
        self.cur = self.conn.cursor()
        self.table_names = ['tweets_stream_ne_i', 'tweets_stream_ne_ii', 'tweets_stream_nw_i', 'tweets_stream_nw_ii', 
                                        'tweets_stream_se', 'tweets_stream_nw_iii', 'tweets_stream_sw']

    def get_rate(self, table):
        sql = "select count(id) from %s where insert_time >= (current_timestamp - interval '1 hours');" % table
        self.cur.execute(sql)
        rate = self.cur.fetchone()[0]
        return (rate, datetime.now())

    def get_total_tweets(self, table):
        sql = "select count(id) from %s" % table
        self.cur.execute(sql)
        num = self.cur.fetchone()[0]
        return (num, datetime.now())

    def generate_report(self):
        report = []
        for table in self.table_names:
            curr_rate = self.get_rate(table)
            total = self.get_total_tweets(table)
            table_data = {
                "name": table, 
                "curr_rate": {
                    "value": curr_rate[0],
                    "time": str(curr_rate[1])
                },
                "total": {
                    "value": total[0],
                    "time": str(total[1])
                }
            }
            report.append(table_data)
        return report

    def write_out(self, report):
        for table in report:
            path = os.path.join(self.params['data_dir'], table['name'] + '.csv')
            if os.path.exists(path):
                f = open(path, 'a')
            else:
                f = open(path, 'w')
                f.write('total,curr_rate,total_time,rate_time\n')
            f.write(str(table['total']['value']) + ',' + str(table['curr_rate']['value']) + ',' + table['curr_rate']['time'] + ',' + table['total']['time'] + '\n')
            f.close()

    def generate_dashboard(self, report):
        from jinja2 import FileSystemLoader, Environment, Template
        templateEnv = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__),'templates')))
        template = templateEnv.get_template("dash.html")
        outputText = template.render(
                generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                host = self.params['db_host'],
                db_name = self.params['db_name'],
                total_num = str(sum([t['total']['value'] for t in report])),
                reports = report
            )
        with open('report_generated.html', 'w') as f:
            f.write(outputText)
        
def main():
    gen = ReportGenerator()
    report = gen.generate_report()
    gen.write_out(report)
    gen.generate_dashboard(report)

if __name__ == "__main__":
    main()
