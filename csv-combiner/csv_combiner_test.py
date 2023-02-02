import unittest
import sys
import os
import pandas as pd
from csv_combiner import merge_csv_files


class TestMergeCSVFiles(unittest.TestCase):
    FILE_NAME_1 = "file1.csv"
    FILE_NAME_2 = "file2.csv"
    FILE_NAME_3 = "file3.csv"
    FILE_NAME_4 = "file4.csv"
    EMAIL_HASH = "email_hash"
    CATEGORY = "category"
    TEST_OUTPUT = "test_output.csv"
    FILE_NAME = "filename"
    data_source = []

    def setUp(self):
        # set up four test files: file1, file2, file3 and file4(empty file).
        technologies = {
            self.EMAIL_HASH: ["21d56b6a0", "31d56b6a1", "42d56b6a0"],
            self.CATEGORY: ["Spark", "PySpark", "Python"]
        }
        df = pd.DataFrame(technologies)
        df.to_csv(self.FILE_NAME_1)

        courses = {
            self.EMAIL_HASH: ["81d56b6a0", "91d56b6a0", "66d56b6a0"],
            self.CATEGORY: ["Math", "Music", "Chemistry"]
        }
        df = pd.DataFrame(courses)
        df.to_csv(self.FILE_NAME_2)

        sports = {
            self.EMAIL_HASH: ["81d56b6a0", "91d56b6a0", "66d56b6a0"],
            self.CATEGORY: ["Football", "Tennis", "Basketball"]
        }
        df = pd.DataFrame(sports)
        df.to_csv(self.FILE_NAME_3)

        books = {
            self.EMAIL_HASH: [],
            self.CATEGORY: []
        }
        df = pd.DataFrame(books)
        df.to_csv(self.FILE_NAME_4)

        self.data_source = [technologies, courses, sports, books]

    def tearDown(self):
        os.remove(self.FILE_NAME_1)
        os.remove(self.FILE_NAME_2)
        os.remove(self.FILE_NAME_3)
        os.remove(self.FILE_NAME_4)
        os.remove(self.TEST_OUTPUT)
        self.data_source = []

    def test_merge_csv_files(self):
        """
        1. Verify that when there are three input files provided, they are correctly combined into one file.
        2. All three input files have valid contents to combine.
        """

        # Merge the test files and capture the output
        output = sys.stdout
        try:
            sys.stdout = open(self.TEST_OUTPUT, "w")
            merge_csv_files([self.FILE_NAME_1, self.FILE_NAME_2, self.FILE_NAME_3])
            sys.stdout.close()
            sys.stdout = output

            # Read the merged output file
            df = pd.read_csv(self.TEST_OUTPUT)

            # Check that the merged output is as expected
            expected_email_hash = \
                self.data_source[0][self.EMAIL_HASH] + \
                self.data_source[1][self.EMAIL_HASH] + \
                self.data_source[2][self.EMAIL_HASH]
            expected_category = \
                self.data_source[0][self.CATEGORY] + \
                self.data_source[1][self.CATEGORY] + \
                self.data_source[2][self.CATEGORY]
            expected_filename = \
                [self.FILE_NAME_1] * len(self.data_source[0][self.CATEGORY]) + \
                [self.FILE_NAME_2] * len(self.data_source[1][self.CATEGORY]) + \
                [self.FILE_NAME_3] * len(self.data_source[2][self.CATEGORY])

            self.assertEqual(len(df), len(expected_filename))
            self.assertEqual(df[self.EMAIL_HASH], expected_email_hash)
            self.assertEqual(df[self.CATEGORY], expected_category)
            self.assertEqual(df[self.FILE_NAME], expected_filename)
        finally:
            return

    def test_merge_empty_csv_files(self):
        """
        Verify that when there are two input files provided and one of them has no data, they are correctly combined
        into one file.
        """

        # Merge the test files and capture the output
        output = sys.stdout
        try:
            sys.stdout = open(self.TEST_OUTPUT, "w")
            merge_csv_files([self.FILE_NAME_1, self.FILE_NAME_4])
            sys.stdout.close()
            sys.stdout = output

            # Read the merged output file
            df = pd.read_csv(self.TEST_OUTPUT)

            # Check that the merged output is as expected
            expected_email_hash = \
                self.data_source[0][self.EMAIL_HASH] + \
                self.data_source[3][self.EMAIL_HASH]
            expected_category = \
                self.data_source[0][self.CATEGORY] + \
                self.data_source[3][self.CATEGORY]
            expected_filename = \
                [self.FILE_NAME_1] * len(self.data_source[0][self.CATEGORY]) + \
                [self.FILE_NAME_4] * len(self.data_source[3][self.CATEGORY])

            self.assertEqual(len(df), len(expected_filename))
            self.assertEqual(df[self.EMAIL_HASH], expected_email_hash)
            self.assertEqual(df[self.CATEGORY], expected_category)
            self.assertEqual(df[self.FILE_NAME], expected_filename)
        finally:
            return


if __name__ == "__main__":
    unittest.main()
