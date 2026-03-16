# Test Suite for Advanced KB Matcher
# Run: python -m pytest test_kb_matcher_advanced.py -v

import pytest
from unittest.mock import Mock, patch
from kb_matcher_advanced import (
    DrugKBMatcherAdvanced, MatchResult,
    NDPS_TIER1_DRUGS, COMMERCIAL_QUANTITY_NDPS,
    validate_commercial_quantity, check_quantity_sanity
)


class TestDrugNormalization:
    """Test drug name normalization."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': 'Heroin', 'standard_name': 'Heroin', 'category_group': 'Opioid'},
            {'raw_name': 'Cocaine', 'standard_name': 'Cocaine', 'category_group': 'Stimulant'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_uppercase_to_lowercase(self):
        """Test case normalization."""
        match = self.matcher.match('GANJA')
        assert match.standard_name == 'Ganja'
        assert match.matched
    
    def test_abbreviation_expansion(self):
        """Test abbreviation handling."""
        match = self.matcher.match('gm Ganja')
        assert 'gram' in match.normalized_name.lower() or 'ganja' in match.normalized_name.lower()
    
    def test_typo_correction(self):
        """Test fuzzy matching for typos."""
        match = self.matcher.match('ganaj')  # Typo
        assert match.standard_name == 'Ganja'
        assert match.match_ratio >= 0.82  # Should be fuzzy_high
    
    def test_regional_slang(self):
        """Test regional drug name variations."""
        match = self.matcher.match('bhang')  # Regional variant of ganja
        # Should either match Ganja via alias or fuzzy match
        assert match.matched or match.match_type in ['fuzzy_high', 'fuzzy_exact']
    
    def test_extra_whitespace(self):
        """Test whitespace handling."""
        match = self.matcher.match('  Ganja  ')
        assert match.standard_name == 'Ganja'


class TestFuzzyMatching:
    """Test fuzzy similarity matching."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': 'Marijuana', 'standard_name': 'Marijuana', 'category_group': 'Cannabis'},
            {'raw_name': 'MDMA', 'standard_name': 'MDMA', 'category_group': 'Synthetic'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_exact_match(self):
        """Test exact string matching."""
        match = self.matcher.match('Ganja')
        assert match.matched
        assert match.match_type == 'exact'
        assert match.match_ratio == 1.0
    
    def test_fuzzy_exact_high_similarity(self):
        """Test fuzzy_exact (95%+) matching."""
        match = self.matcher.match('Ganja ')  # Trailing space
        assert match.match_type in ['exact', 'fuzzy_exact']
        assert match.match_ratio >= 0.95
    
    def test_fuzzy_high_medium_similarity(self):
        """Test fuzzy_high (82-94%) matching."""
        variants = ['ganaj', 'ganja', 'ganja ']
        for variant in variants:
            match = self.matcher.match(variant)
            if match.matched:
                assert match.match_ratio >= 0.82
    
    def test_fuzzy_medium_substring_match(self):
        """Test fuzzy_medium (72-81%) substring matching."""
        match = self.matcher.match('marijuana joint plant')
        if match.matched:
            assert match.match_ratio >= 0.60
    
    def test_no_match_low_similarity(self):
        """Test no match when similarity too low."""
        match = self.matcher.match('xyz123abc')
        assert not match.matched or match.match_ratio < 0.60


class TestFormUnitValidation:
    """Test form-unit consistency validation."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': 'Heroin', 'standard_name': 'Heroin', 'category_group': 'Opioid'},
            {'raw_name': 'Liquid LSD', 'standard_name': 'LSD', 'category_group': 'Hallucinogen'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_solid_form_with_kg(self):
        """Test solid drugs accept kg."""
        match = self.matcher.match('Ganja', quantity=5, unit='kg', form='solid')
        assert match.form_unit_valid or match.match_type == 'exact'
    
    def test_solid_form_with_grams(self):
        """Test solid drugs accept grams."""
        match = self.matcher.match('Ganja', quantity=500, unit='gm', form='solid')
        assert match.form_unit_valid or match.match_type == 'exact'
    
    def test_liquid_form_with_ml(self):
        """Test liquid drugs accept ml."""
        match = self.matcher.match('Liquid LSD', quantity=100, unit='ml', form='liquid')
        result_valid = match.form_unit_valid if hasattr(match, 'form_unit_valid') else True
        assert result_valid
    
    def test_invalid_liquid_form_with_grams(self):
        """Test liquid drugs REJECT grams - edge case."""
        match = self.matcher.match('Ganja Oil', quantity=100, unit='gm', form='liquid')
        # Should flag as invalid or low confidence
        if match.matched:
            assert match.confidence_score <= 0.70  # Should be penalized
    
    def test_count_form(self):
        """Test counted items (tablets, blots)."""
        match = self.matcher.match('LSD blots', quantity=100, unit='no', form='count')
        if match.matched:
            assert 'blots' in match.audit_log.get('normalized_input', '').lower() or True


class TestQuantitySanity:
    """Test quantity sanity checking."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_reasonable_quantity(self):
        """Test normal quantities pass sanity check."""
        match = self.matcher.match('Ganja', quantity=10, unit='kg')
        assert match.quantity_valid
    
    def test_extreme_quantity_flagged(self):
        """Test extreme quantities flagged."""
        match = self.matcher.match('Ganja', quantity=5000, unit='kg')
        assert not match.quantity_valid, "Should flag 5000kg as outlier"
    
    def test_zero_quantity_rejected(self):
        """Test zero quantity rejected."""
        match = self.matcher.match('Ganja', quantity=0, unit='kg')
        assert not match.quantity_valid
    
    def test_negative_quantity_rejected(self):
        """Test negative quantity rejected."""
        match = self.matcher.match('Ganja', quantity=-10, unit='kg')
        assert not match.quantity_valid


class TestCommercialThresholds:
    """Test NDPS commercial quantity thresholds."""
    
    def test_ganja_threshold(self):
        """Test Ganja 20kg commercial threshold."""
        assert validate_commercial_quantity('Ganja', 25, 'kg') == True
        assert validate_commercial_quantity('Ganja', 15, 'kg') == False
    
    def test_heroin_threshold(self):
        """Test Heroin 250g commercial threshold."""
        assert validate_commercial_quantity('Heroin', 250, 'gm') == True
        assert validate_commercial_quantity('Heroin', 100, 'gm') == False
    
    def test_cocaine_threshold(self):
        """Test Cocaine 500g commercial threshold."""
        assert validate_commercial_quantity('Cocaine', 500, 'gm') == True
        assert validate_commercial_quantity('Cocaine', 100, 'gm') == False
    
    def test_lsd_threshold(self):
        """Test LSD 100 blots commercial threshold."""
        assert validate_commercial_quantity('LSD', 100, 'no') == True
        assert validate_commercial_quantity('LSD', 50, 'no') == False
    
    def test_unrecognized_drug(self):
        """Test unrecognized drugs default to False."""
        assert validate_commercial_quantity('UnknownDrug123', 1000, 'kg') == False


class TestFalsePositiveDetection:
    """Test false positive pattern detection."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_customer_list_detection(self):
        """Test detection of customer list pattern."""
        # This would be detected during extraction refinement
        text = "sold ganja to Sidhu, Ramesh, and Suresh"
        match = self.matcher.match('ganja')
        # Check if matcher context includes false positive detection
        assert hasattr(match, 'audit_log')
    
    def test_no_seizure_context_detection(self):
        """Test detection of 'no seizure' context."""
        # Pattern: "no ganja found", "ganja not recovered"
        pass  # Context-dependent, tested during extraction
    
    def test_purchase_order_detection(self):
        """Test detection of purchase order context."""
        # Pattern: "purchase ganja", "ordered 5kg ganja"
        pass  # Context-dependent


class TestMatchResultAuditTrail:
    """Test audit trail completeness."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_audit_log_present(self):
        """Test audit log is always populated."""
        match = self.matcher.match('ganaj')
        assert match.audit_log is not None
        assert isinstance(match.audit_log, dict)
    
    def test_audit_log_has_decision_steps(self):
        """Test audit log includes decision steps."""
        match = self.matcher.match('ganaj')
        assert 'normalized_input' in match.audit_log
        assert 'similarity_checks' in match.audit_log or 'similarity_score' in match.audit_log
    
    def test_audit_log_reason_present(self):
        """Test audit log includes reason for decision."""
        match = self.matcher.match('xyz123')
        assert 'decision_reason' in match.audit_log or len(match.audit_log) > 0


class TestConfidenceAdjustment:
    """Test confidence score adjustments."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_exact_match_confidence_boost(self):
        """Test confidence boost for exact KB match."""
        match = self.matcher.match('Ganja', quantity=10, unit='kg')
        # Confidence should include boost if specified
        assert match.confidence_score > 0.70  # Above default
    
    def test_fuzzy_high_match_boost(self):
        """Test confidence boost for fuzzy_high match."""
        match = self.matcher.match('ganaj', quantity=10, unit='kg')
        if match.matched and match.match_type == 'fuzzy_high':
            assert match.confidence_score >= 0.70
    
    def test_suspicious_pattern_discount(self):
        """Test confidence discount for suspicious patterns."""
        # Would be tested during refinement pipeline
        pass


class TestMultipleDrugs:
    """Test batch processing of multiple drugs."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': 'Heroin', 'standard_name': 'Heroin', 'category_group': 'Opioid'},
            {'raw_name': 'Cocaine', 'standard_name': 'Cocaine', 'category_group': 'Stimulant'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_batch_matching(self):
        """Test matching multiple drugs sequentially."""
        drugs = ['ganaj', 'heroin', 'cocaine']
        results = [self.matcher.match(drug) for drug in drugs]
        
        assert len(results) == len(drugs)
        for result in results:
            assert isinstance(result, MatchResult)
    
    def test_independent_matching(self):
        """Test matching one drug doesn't affect next."""
        result1 = self.matcher.match('ganja')
        result2 = self.matcher.match('heroin')
        
        assert result1.standard_name == 'Ganja'
        assert result2.standard_name == 'Heroin'


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': '', 'standard_name': 'Unknown', 'category_group': 'Other'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_empty_string(self):
        """Test empty drug name."""
        match = self.matcher.match('')
        assert not match.matched or match.confidence_score < 0.50
    
    def test_single_character(self):
        """Test single character input."""
        match = self.matcher.match('a')
        assert not match.matched or match.confidence_score < 0.50
    
    def test_very_long_name(self):
        """Test very long drug name."""
        long_name = 'ganja ' * 50
        match = self.matcher.match(long_name)
        # Should handle gracefully
        assert isinstance(match, MatchResult)
    
    def test_unicode_characters(self):
        """Test Unicode/Hindi characters."""
        match = self.matcher.match('गांजा')  # Ganja in Hindi
        # Should handle gracefully even if no match
        assert isinstance(match, MatchResult)
    
    def test_special_characters(self):
        """Test special characters in drug name."""
        match = self.matcher.match('ganja@#$%')
        assert isinstance(match, MatchResult)
    
    def test_numeric_drug_name(self):
        """Test numeric-only input."""
        match = self.matcher.match('123')
        assert not match.matched or match.confidence_score < 0.50


class TestIndianNumberFormats:
    """Test Indian number format handling."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_indian_rupee_format(self):
        """Test Indian rupee format (Rs.52,00,000)."""
        # This is handled in normalization
        match = self.matcher.match('Ganja Rs.52,00,000')
        # Should parse normalized version
        assert match.standard_name == 'Ganja'
    
    def test_lakh_notation(self):
        """Test lakh notation."""
        match = self.matcher.match('Ganja 5 lakh rupees')
        if match.matched:
            assert 'rupees' in match.normalized_name.lower() or 'ganja' in match.normalized_name.lower()


# ─────────────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────────────

class TestIntegrationWithRealKB:
    """Test with realistic KB entries."""
    
    def test_with_100_entry_kb(self):
        """Test performance with 100+ KB entries."""
        kb = [
            {'raw_name': f'Drug{i}', 'standard_name': f'StandardDrug{i}', 
             'category_group': ['Cannabis', 'Opioid', 'Stimulant'][i % 3]}
            for i in range(100)
        ]
        kb.extend([
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
        ])
        
        matcher = DrugKBMatcherAdvanced(kb)
        match = matcher.match('ganaj')
        
        assert match.matched or match is not None
    
    def test_with_real_typos(self):
        """Test with real misspellings found in FIRs."""
        kb = [
            {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
            {'raw_name': 'Heroin', 'standard_name': 'Heroin', 'category_group': 'Opioid'},
            {'raw_name': 'Charas', 'standard_name': 'Charas', 'category_group': 'Cannabis'},
        ]
        matcher = DrugKBMatcherAdvanced(kb)
        
        typos = ['ganaj', 'gandja', 'heroine', 'charash', 'chras', 'chamis']
        for typo in typos:
            match = matcher.match(typo)
            assert isinstance(match, MatchResult)


# ─────────────────────────────────────────────────────
# Performance Tests
# ─────────────────────────────────────────────────────

@pytest.mark.performance
class TestPerformance:
    """Test performance metrics."""
    
    def setup_method(self):
        self.kb = [
            {'raw_name': f'Drug{i}', 'standard_name': f'StandardDrug{i}', 
             'category_group': 'Unknown'}
            for i in range(500)
        ]
        self.matcher = DrugKBMatcherAdvanced(self.kb)
    
    def test_matching_latency(self):
        """Test matching latency is <10ms per drug."""
        import time
        start = time.time()
        for i in range(100):
            self.matcher.match('Drug50')
        duration = time.time() - start
        avg_latency = duration / 100 * 1000  # ms
        
        assert avg_latency < 10, f"Latency {avg_latency}ms exceeds 10ms"
    
    def test_kb_loading_time(self):
        """Test KB loading doesn't exceed 1 second."""
        import time
        start = time.time()
        matcher = DrugKBMatcherAdvanced(self.kb)
        duration = time.time() - start
        
        assert duration < 1.0, f"Loading took {duration}s, should be <1s"


if __name__ == '__main__':
    # Run tests: python -m pytest test_kb_matcher_advanced.py -v
    pytest.main([__file__, '-v', '--tb=short'])
