import unittest
from anygrate import depending


class TestDepending(unittest.TestCase):

    """ Tests """

    def setUp(self):
        super(TestDepending, self).setUp()

    def test_get_dependencies(self):
        """ Method to verify that the dependency order is right """

        res_country = depending.add_related_tables_to_models_dependencies('admin',
                                                 'admin', 'test',
                                                 ('res.country',), None)

        res_account = depending.add_related_tables_to_models_dependencies('admin', 'admin',
                                                 'test',
                                                 ('account.account',), ('ir.model',),
                                                 None)

        res_groups_ex = depending.add_related_tables_to_models_dependencies('admin', 'admin',
                                                   'test',
                                                   ('res.groups',),
                                                   ('ir.module.category', 'ir.model'),
                                                   None)

        self.assertTrue(res_country is not None)
        self.assertTrue(res_account is not None)
        self.assertTrue(res_groups_ex is not None)
        self.assertTrue('res.users' in res_account)
        self.assertFalse('ir.model' in res_account)
        self.assertTrue('res.users' in res_groups_ex)
        self.assertTrue('res_groups_users_rel' in res_groups_ex)
        self.assertFalse('ir.module.category' in res_groups_ex)
        self.assertEquals(res_country, ['res.country'])
        self.assertEquals(res_account, ['account.financial.report',
                                        'account.tax.code',
                                        'account.tax',
                                        'account.account.type',
                                        'res.currency',
                                        'ir.ui.menu',
                                        'ir.rule',
                                        'ir.module.category',
                                        'res.groups',
                                        'ir.actions',
                                        'res.users',
                                        'res.country.state',
                                        'res.partner.category',
                                        'account.payment.term',
                                        'res.partner.title',
                                        'account.fiscal.position',
                                        'product.pricelist',
                                        'res.partner',
                                        'res.country',
                                        'res.company',
                                        'account.account',
                                        'ir_ui_menu_group_rel',
                                        'res_company_users_rel',
                                        'rule_group_rel',
                                        'account_account_financial_report',
                                        'res_partner_category_rel',
                                        'res_groups_users_rel',
                                        'account_account_financial_report_type',
                                        'res_groups_implied_rel',
                                        'account_account_consol_rel',
                                        'account_account_tax_default_rel'])

        self.assertEquals(res_groups_ex, ['ir.ui.menu',
                                          'account.financial.report',
                                          'account.tax.code',
                                          'account.tax',
                                          'account.account.type',
                                          'account.account',
                                          'res.country.state',
                                          'res.country',
                                          'res.currency',
                                          'res.partner.category',
                                          'account.payment.term',
                                          'res.partner.title',
                                          'account.fiscal.position',
                                          'product.pricelist',
                                          'res.partner',
                                          'res.company',
                                          'ir.actions',
                                          'res.users',
                                          'ir.rule',
                                          'res.groups',
                                          'account_account_financial_report_type',
                                          'res_company_users_rel',
                                          'rule_group_rel',
                                          'account_account_financial_report',
                                          'res_partner_category_rel',
                                          'res_groups_users_rel',
                                          'ir_ui_menu_group_rel',
                                          'res_groups_implied_rel',
                                          'account_account_consol_rel',
                                          'account_account_tax_default_rel'])
