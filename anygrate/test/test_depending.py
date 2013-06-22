import unittest
from anygrate import depending
import psycopg2


class TestDepending(unittest.TestCase):

    """ Tests """

    def setUp(self):
        super(TestDepending, self).setUp()

    def test_get_dependencies(self):
        """ Verify that the dependency order is right.
        This test should be done on a base with account module installed
        """

        connection = psycopg2.connect("dbname=test")
        country, _ = depending.add_related_tables(connection,
                                                  ['res_country'], None)

        account, _ = depending.add_related_tables(connection,
                                                  ['account_account'],
                                                  ['ir_model'],
                                                  None)

        groups_excl, _ = depending.add_related_tables(connection,
                                                      ['res_groups'],
                                                      ['ir_module_category',
                                                      'ir_model'],
                                                      None)

        self.assertTrue(country is not None)
        self.assertTrue(account is not None)
        self.assertTrue(groups_excl is not None)
        self.assertTrue('res_users' in account,
                        u"Did you forget to install 'account' in the 'test' db?")
        self.assertFalse('ir_model' in account)
        self.assertTrue('res_users' in groups_excl)
        self.assertTrue('res_groups_users_rel' in groups_excl)
        self.assertFalse('ir_module_category' in groups_excl)
        self.assertEquals(country, ['res_users', 'res_country'])
        self.assertEquals(account, ['res_users', 'res_partner_title',
                                    'res_partner', 'res_company',
                                    'res_currency', 'account_account_type',
                                    'account_account',
                                    'account_account_consol_rel'])
        self.assertEquals(groups_excl, ['res_users', 'res_groups',
                                        'res_groups_implied_rel',
                                        'res_groups_users_rel'])
