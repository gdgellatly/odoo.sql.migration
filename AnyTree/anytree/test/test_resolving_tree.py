import unittest
from anytree import anytree


class TestResolvingTree(unittest.TestCase):

    """ Tests """

    def setUp(self):
        super(TestResolvingTree, self).setUp()

    def test_get_ordre_import(self):

        res_country = anytree.get_ordre_importation('admin', 'admin', 'ecox_db', ('res.country',),
                                                    None, None)
        res_account_account = anytree.get_ordre_importation('admin', 'admin', 'ecox_db',
                                                            ('account.account',), None, None)

        res_groups_exclusion = anytree.get_ordre_importation('admin', 'admin', 'ecox_db',
                                                             ('res.groups',),
                                                             ['ir.module.category'], None)

        res_groups = anytree.get_ordre_importation('admin', 'admin', 'ecox_db', ('res.groups',), None,
                                                  None)

        self.assertEquals(res_country, ['res.country'])

        self.assertEquals(res_account_account, ['account.financial.report',
        'account_account_financial_report_type', 'account.tax.code',
        'account.tax', 'account.account.type', 'res.currency', 'ir.ui.menu',
        'ir.model', 'ir.rule', 'ir.module.category', 'res.groups',
        'ir_ui_menu_group_rel', 'res_groups_implied_rel', 'rule_group_rel',
        'ir.sequence', 'account.journal.view', 'account.analytic.journal',
        'account.journal', 'account_journal_group_rel',
        'account_journal_type_rel', 'account_account_type_rel',
        'account.payment.term', 'product.pricelist', 'stock.journal',
        'stock.location', 'res.partner.title', 'res.partner.address',
        'stock.warehouse', 'account.analytic.account', 'sale.shop',
        'shop.term', 'shop_journal_rel', 'ir.actions.actions', 'res.users',
        'res_groups_users_rel', 'res.country.state', 'res.country',
        'report.stylesheets', 'res.partner.category', 'account.fiscal.position',
        'res.partner', 'res_partner_category_rel', 'res.company',
        'res_company_users_rel', 'account.account',
        'account_account_consol_rel', 'account_account_financial_report',
        'account_account_tax_default_rel'])

        self.assertEquals(res_groups_exclusion, ['ir.ui.menu',
        'account.account.type', 'account.financial.report',
        'account_account_financial_report_type', 'account.tax.code',
        'account.tax', 'account.account', 'account_account_consol_rel',
        'account_account_financial_report', 'account_account_tax_default_rel',
        'ir.sequence', 'account.journal.view', 'res.currency',
        'account.analytic.journal', 'account.journal',
        'account_journal_group_rel', 'account_journal_type_rel',
        'account_account_type_rel', 'account.payment.term',
        'product.pricelist', 'stock.journal', 'stock.location',
        'res.country.state', 'res.country', 'res.partner.category',
        'account.fiscal.position', 'res.partner', 'res_partner_category_rel',
        'res.partner.title', 'res.partner.address', 'stock.warehouse',
        'account.analytic.account', 'sale.shop', 'shop.term',
        'shop_journal_rel', 'report.stylesheets', 'res.company',
        'ir.actions.actions', 'res.users', 'res_company_users_rel', 'ir.model',
        'ir.rule', 'res.groups', 'ir_ui_menu_group_rel',
        'res_groups_users_rel', 'res_groups_implied_rel', 'rule_group_rel'])

        self.assertEquals(res_groups, ['ir.ui.menu',
        'account.account.type', 'account.financial.report',
        'account_account_financial_report_type', 'account.tax.code',
        'account.tax', 'account.account', 'account_account_consol_rel',
        'account_account_financial_report', 'account_account_tax_default_rel',
        'ir.sequence', 'account.journal.view', 'res.currency',
        'account.analytic.journal', 'account.journal',
        'account_journal_group_rel', 'account_journal_type_rel',
        'account_account_type_rel', 'account.payment.term',
        'product.pricelist', 'stock.journal', 'stock.location',
        'res.country.state', 'res.country', 'res.partner.category',
        'account.fiscal.position', 'res.partner', 'res_partner_category_rel',
        'res.partner.title', 'res.partner.address', 'stock.warehouse',
        'account.analytic.account', 'sale.shop', 'shop.term',
        'shop_journal_rel', 'report.stylesheets', 'res.company',
        'ir.actions.actions', 'res.users', 'res_company_users_rel', 'ir.model',
        'ir.rule', 'ir.module.category', 'res.groups', 'ir_ui_menu_group_rel',
        'res_groups_users_rel', 'res_groups_implied_rel', 'rule_group_rel'])
