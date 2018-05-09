import luigi
# python -m luigi --module top_artists AggregateArtists --local-scheduler --date-interval 2017-10
# luigi --module top_artists AggregateArtists --local-scheduler --date-interval 2017-10

class MyTask(luigi.ExternalTask):

    param = luigi.Parameter(default=42)

    def run(self):
        f = self.output().open('w')
        f.write("hello world from run")
        f.close()

    def output(self):
        return luigi.LocalTarget('./tmp/foo/bar-{}.txt'.format(str(self.param)))

if __name__ == '__main__':
    luigi.run(['MyTask', '--local-scheduler'])
