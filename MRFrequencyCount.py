from mrjob.job import MRJob


class MRPeopleFrequencyCount(MRJob):

    def mapper(self, _, line):
        (Gender,ZipCode) = line.split(',')

        if int(ZipCode) == 19103:
            yield "People", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRPeopleFrequencyCount.run()