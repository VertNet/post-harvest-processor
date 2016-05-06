import unittest
from trait_parsers.body_mass_parser import BodyMassParser


class TestBodyMassParser(unittest.TestCase):

    def test_1(self):
        self.assertDictEqual(
            target.parse('762-292-121-76 2435.0g'),
            {'key': '_shorthand_', 'value': '2435.0', 'units': 'g'})

    def test_2(self):
        self.assertDictEqual(
            target.parse('TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx'),
            {'key': 'Weight', 'value': '0.77', 'units': 'g'})

    def test_3(self):
        self.assertDictEqual(
            target.parse('Note in catalog: Mus. SW Biol. NK 30009; 91-0-17-22-62g'),
            {'key': '_shorthand_', 'value': '62', 'units': 'g'})

    def test_4(self):
        self.assertDictEqual(
            target.parse('body mass=20 g'),
            {'key': 'body mass', 'value': '20', 'units': 'g'})

    def test_5(self):
        self.assertDictEqual(
            target.parse('2 lbs. 3.1 - 4.5 oz '),
            {'key': '_english_', 'value': ['2', '3.1 - 4.5'], 'units': ['lbs.', 'oz']})

    def test_6(self):
        self.assertDictEqual(
            target.parse('{"totalLengthInMM":"x", "earLengthInMM":"20", "weight":"[139.5] g" }'),
            {'key': 'weight', 'value': '[139.5]', 'units': 'g'})

    def test_7(self):
        self.assertDictEqual(
            target.parse('{"fat":"No fat", "gonads":"Testes 10 x 6 mm.", "molt":"No molt",'
                         ' "stomach contents":"Not recorded", "weight":"94 gr."'),
            {'key': 'weight', 'value': '94', 'units': 'gr.'})

    def test_8(self):
        self.assertDictEqual(
            target.parse('Note in catalog: 83-0-17-23-fa64-35g'),
            {'key': '_shorthand_', 'value': '35', 'units': 'g'})

    def test_9(self):
        self.assertDictEqual(
            target.parse('{"measurements":"20.2g, SVL 89.13mm" }'),
            {'key': 'measurements', 'value': '20.2', 'units': 'g'})

    def test_10(self):
        self.assertDictEqual(
            target.parse('Body: 15 g'),
            {'key': 'Body', 'value': '15', 'units': 'g'})

    def test_11(self):
        self.assertDictEqual(
            target.parse('82-00-15-21-tr7-fa63-41g'),
            {'key': '_shorthand_', 'value': '41', 'units': 'g'})

    def test_12(self):
        self.assertDictEqual(
            target.parse('weight=5.4 g; unformatted measurements=77-30-7-12=5.4'),
            {'key': 'weight', 'value': '5.4', 'units': 'g'})

    def test_13(self):
        self.assertDictEqual(
            target.parse('unformatted measurements=77-30-7-12=5.4; weight=5.4;'),
            {'key': 'measurements', 'value': '5.4', 'units': None})

    def test_14(self):
        self.assertDictEqual(
            target.parse('{"totalLengthInMM":"270-165-18-22-31", '),
            {'key': '_shorthand_', 'value': '31', 'units': None})

    def test_15(self):
        self.assertDictEqual(
            target.parse('{"measurements":"143-63-20-17=13 g" }'),
            {'key': 'measurements', 'value': '13', 'units': 'g'})

    def test_16(self):
        self.assertDictEqual(
            target.parse('143-63-20-17=13'),
            {'key': '_shorthand_', 'value': '13', 'units': None})

    def test_17(self):
        self.assertDictEqual(
            target.parse('reproductive data: Testes descended -10x7 mm; sex: male;'
                         ' unformatted measurements: 181-75-21-18=22 g'),
            {'key': 'measurements', 'value': '22', 'units': 'g'})

    def test_18(self):
        self.assertDictEqual(
            target.parse('{ "massInGrams"="20.1" }'),
            {'key': 'massInGrams', 'value': '20.1', 'units': 'Grams'})

    def test_19(self):
        self.assertDictEqual(
            target.parse(' {"gonadLengthInMM_1":"10", "gonadLengthInMM_2":"6", "weight":"1,192.0" }'),
            {'key': 'weight', 'value': '1,192.0', 'units': None})

    def test_20(self):
        self.assertDictEqual(
            target.parse('"weight: 20.5-31.8'),
            {'key': 'weight', 'value': '20.5-31.8', 'units': None})

    def test_21(self):
        self.assertDictEqual(
            target.parse('"weight: 20.5-32'),
            {'key': 'weight', 'value': '20.5-32', 'units': None})

    def test_22(self):
        self.assertDictEqual(
            target.parse('"weight: 21-31.8'),
            {'key': 'weight', 'value': '21-31.8', 'units': None})

    def test_23(self):
        self.assertDictEqual(
            target.parse('"weight: 21-32'),
            {'key': 'weight', 'value': '21-32', 'units': None})

    def test_24(self):
        self.assertEqual(
            target.parse("Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,"
                         "5592,5595,5594,5593,5596,5589,5587,5586,5585"),
            None)

    def test_25(self):
        self.assertDictEqual(
            target.parse('weight=5.4 g; unformatted measurements=77-x-7-12=5.4'),
            {'key': 'weight', 'value': '5.4', 'units': 'g'})

    def test_26(self):
        self.assertEqual(
            target.parse('c701563b-dbd9-4500-184f-1ad61eb8da11'),
            None)

    ######################################################################
    ######################################################################
    ######################################################################
    ######################################################################

    def test_1(self):
        self.assertDictEqual(
            target.search_and_normalize(['762-292-121-76 2435.0g']),
            {'hasMass': 1, 'massInG': 2435.0, 'wereMassUnitsInferred': 0})

    def test_2(self):
        self.assertDictEqual(
            target.search_and_normalize(['TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx']),
            {'hasMass': 1, 'massInG': 0.77, 'wereMassUnitsInferred': 0})

    def test_3(self):
        self.assertDictEqual(
            target.search_and_normalize(['Note in catalog: Mus. SW Biol. NK 30009; 91-0-17-22-62g']),
            {'hasMass': 1, 'massInG': 62, 'wereMassUnitsInferred': 0})

    def test_4(self):
        self.assertDictEqual(
            target.search_and_normalize(['body mass=20 g']),
            {'hasMass': 1, 'massInG': 20, 'wereMassUnitsInferred': 0})

    def test_5(self):
        self.assertDictEqual(
            target.search_and_normalize(['2 lbs. 3.1 - 4.5 oz ']),
            {'hasMass': 1, 'massInG': 0, 'wereMassUnitsInferred': 0})

    def test_6(self):
        self.assertDictEqual(
            target.search_and_normalize(['{"totalLengthInMM":"x", "earLengthInMM":"20", "weight":"[139.5] g" }']),
            {'hasMass': 1, 'massInG': 139.5, 'wereMassUnitsInferred': 0})

    def test_7(self):
        self.assertDictEqual(
            target.search_and_normalize(['{"fat":"No fat", "gonads":"Testes 10 x 6 mm.", "molt":"No molt",'
                                         ' "stomach contents":"Not recorded", "weight":"94 gr."']),
            {'hasMass': 1, 'massInG': 94, 'wereMassUnitsInferred': 0})

    def test_8(self):
        self.assertDictEqual(
            target.search_and_normalize(['Note in catalog: 83-0-17-23-fa64-35g']),
            {'hasMass': 1, 'massInG': 35, 'wereMassUnitsInferred': 0})

    def test_9(self):
        self.assertDictEqual(
            target.search_and_normalize(['{"measurements":"20.2g, SVL 89.13mm" }']),
            {'hasMass': 1, 'massInG': 20.2, 'wereMassUnitsInferred': 0})

    def test_10(self):
        self.assertDictEqual(
            target.search_and_normalize(['Body: 15 g']),
            {'hasMass': 1, 'massInG': 15, 'wereMassUnitsInferred': 0})

    def test_11(self):
        self.assertDictEqual(
            target.search_and_normalize(['82-00-15-21-tr7-fa63-41g']),
            {'hasMass': 1, 'massInG': 41, 'wereMassUnitsInferred': 0})

    def test_12(self):
        self.assertDictEqual(
            target.search_and_normalize(['weight=5.4 g; unformatted measurements=77-30-7-12=5.4']),
            {'hasMass': 1, 'massInG': 5.4, 'wereMassUnitsInferred': 0})

    def test_13(self):
        self.assertDictEqual(
            target.search_and_normalize(['unformatted measurements=77-30-7-12=5.4; weight=5.4;']),
            {'hasMass': 1, 'massInG': 5.4, 'wereMassUnitsInferred': 1})

    def test_14(self):
        self.assertDictEqual(
            target.search_and_normalize(['{"totalLengthInMM":"270-165-18-22-31", ']),
            {'hasMass': 1, 'massInG': 31, 'wereMassUnitsInferred': 1})

    def test_15(self):
        self.assertDictEqual(
            target.search_and_normalize(['{"measurements":"143-63-20-17=13 g" }']),
            {'hasMass': 1, 'massInG': 13, 'wereMassUnitsInferred': 0})

    def test_16(self):
        self.assertDictEqual(
            target.search_and_normalize(['143-63-20-17=13']),
            {'hasMass': 1, 'massInG': 13, 'wereMassUnitsInferred': 1})

    def test_17(self):
        self.assertDictEqual(
            target.search_and_normalize(['reproductive data: Testes descended -10x7 mm; sex: male;'
                                         ' unformatted measurements: 181-75-21-18=22 g']),
            {'hasMass': 1, 'massInG': 22, 'wereMassUnitsInferred': 0})

    def test_18(self):
        self.assertDictEqual(
            target.search_and_normalize(['{ "massInGrams"="20.1" }']),
            {'hasMass': 1, 'massInG': 20.1, 'wereMassUnitsInferred': 0})

    def test_19(self):
        self.assertDictEqual(
            target.search_and_normalize([' {"gonadLengthInMM_1":"10", "gonadLengthInMM_2":"6", "weight":"1,192.0" }']),
            {'hasMass': 1, 'massInG': 1192.0, 'wereMassUnitsInferred': 1})

    def test_20(self):
        self.assertDictEqual(
            target.search_and_normalize(['"weight: 20.5-31.8']),
            {'hasMass': 1, 'massInG': 0, 'wereMassUnitsInferred': 1})

    def test_21(self):
        self.assertDictEqual(
            target.search_and_normalize(['"weight: 20.5-32']),
            {'hasMass': 1, 'massInG': 0, 'wereMassUnitsInferred': 1})

    def test_22(self):
        self.assertDictEqual(
            target.search_and_normalize(['"weight: 21-31.8']),
            {'hasMass': 1, 'massInG': 0, 'wereMassUnitsInferred': 1})

    def test_23(self):
        self.assertDictEqual(
            target.search_and_normalize(['"weight: 21-32']),
            {'hasMass': 1, 'massInG': 0, 'wereMassUnitsInferred': 1})

    def test_24(self):
        self.assertEqual(
            target.search_and_normalize(["Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,"
                                         "5592,5595,5594,5593,5596,5589,5587,5586,5585"]),
            {'hasMass': 0, 'massInG': 0, 'wereMassUnitsInferred': 0})

    def test_25(self):
        self.assertDictEqual(
            target.search_and_normalize(['weight=5.4 g; unformatted measurements=77-x-7-12=5.4']),
            {'hasMass': 1, 'massInG': 5.4, 'wereMassUnitsInferred': 0})

    def test_26(self):
        self.assertEqual(
            target.search_and_normalize(['c701563b-dbd9-4500-184f-1ad61eb8da11']),
            {'hasMass': 0, 'massInG': 0, 'wereMassUnitsInferred': 0})


target = BodyMassParser()
suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBodyMassParser)
unittest.TextTestRunner().run(suite)
