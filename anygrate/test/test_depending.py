import unittest
from anygrate import depending
import psycopg2


class TestDepending(unittest.TestCase):

    """ Tests """

    def setUp(self):
        super(TestDepending, self).setUp()

    def test_get_dependencies(self):
        """ Method to verify that the dependency order is right """
        target_db = 'test'
        connection = psycopg2.connect("dbname=%s" % target_db)

        res_country = depending.get_dependencies(connection, ('res_country',),
                                                 None, None)
        res_account = depending.get_dependencies(connection, ('account_account',),
                                                 None, None)

        res_groups_ex = depending.get_dependencies(connection, ('res_groups',),
                                                   ['ir_module_category'],
                                                   None)

        res_groups = depending.get_dependencies(connection, ('res_groups',), None,
                                                None)

        self.assertEquals(res_country, ['res.country'])

        # /!\ ATTENTION, CES ASSERTIONS DEPENDENT DES MODULES INSTALLES /!\

        self.assertEquals(res_account, [
            'account.financial.report',
            'account_account_financial_report_type', 'account.tax.code',
            'account.tax', 'account.account.type', 'res.currency', 'ir.ui.menu',
            'ir.model', 'ir.rule', 'ir.module.category', 'res.groups', 'ir_ui_menu_group_rel',
            'res_groups_implied_rel', 'rule_group_rel', 'ir.actions', 'res.users',
            'res_groups_users_rel', 'res.country.state', 'res.partner.category',
            'account.payment.term', 'res.partner.title', 'account.fiscal.position',
            'product.pricelist', 'res.partner', 'res_partner_category_rel',
            'res.country', 'res.company', 'res_company_users_rel', 'account.account',
            'account_account_consol_rel', 'account_account_financial_report',
            'account_account_tax_default_rel'])

        self.assertEquals(res_groups_ex, [
            'ir.ui.menu', 'account.financial.report',
            'account_account_financial_report_type', 'account.tax.code', 'account.tax',
            'account.account.type', 'account.account', 'account_account_consol_rel',
            'account_account_financial_report', 'account_account_tax_default_rel',
            'res.country.state', 'res.country', 'res.currency', 'res.partner.category',
            'account.payment.term', 'res.partner.title', 'account.fiscal.position',
            'product.pricelist', 'res.partner', 'res_partner_category_rel', 'res.company',
            'ir.actions', 'res.users', 'res_company_users_rel', 'ir.model', 'ir.rule',
            'res.groups', 'ir_ui_menu_group_rel', 'res_groups_users_rel',
            'res_groups_implied_rel', 'rule_group_rel'])

        self.assertEquals(res_groups, [
            'ir.ui.menu', 'account.financial.report',
            'account_account_financial_report_type', 'account.tax.code', 'account.tax',
            'account.account.type', 'account.account', 'account_account_consol_rel',
            'account_account_financial_report', 'account_account_tax_default_rel',
            'res.country.state', 'res.country', 'res.currency', 'res.partner.category',
            'account.payment.term', 'res.partner.title', 'account.fiscal.position',
            'product.pricelist', 'res.partner', 'res_partner_category_rel', 'res.company',
            'ir.actions', 'res.users', 'res_company_users_rel', 'ir.model', 'ir.rule',
            'ir.module.category', 'res.groups', 'ir_ui_menu_group_rel', 'res_groups_users_rel',
            'res_groups_implied_rel', 'rule_group_rel'])
