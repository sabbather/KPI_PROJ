import unittest
from datetime import date, timedelta

import pandas as pd

from streamlit_app import compute_kpi1_score, weighted_hours_ratio


class Kpi1ScoringTests(unittest.TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.due = self.start + timedelta(days=100)

    def score_completed(self, elapsed_days, tolerance_pct=20):
        return compute_kpi1_score(
            start_date=self.start,
            due_date=self.due,
            completed=True,
            completed_date=self.start + timedelta(days=elapsed_days),
            tolerance_pct=tolerance_pct,
        )

    def test_completion_within_plan_scores_one(self):
        self.assertEqual(self.score_completed(100), 1.0)

    def test_score_decreases_linearly_within_tolerance(self):
        self.assertAlmostEqual(self.score_completed(110), 0.5)

    def test_completion_at_end_of_tolerance_scores_zero(self):
        self.assertEqual(self.score_completed(120), 0.0)

    def test_open_overdue_task_is_not_scored(self):
        score = compute_kpi1_score(
            start_date=self.start,
            due_date=self.due,
            completed=False,
            completed_date=None,
            tolerance_pct=20,
        )
        self.assertIsNone(score)

    def test_zero_tolerance_gives_zero_after_planned_duration(self):
        self.assertEqual(self.score_completed(100, tolerance_pct=0), 1.0)
        self.assertEqual(self.score_completed(101, tolerance_pct=0), 0.0)

    def test_missing_completion_date_cannot_be_scored(self):
        self.assertIsNone(
            compute_kpi1_score(
                start_date=self.start,
                due_date=self.due,
                completed=True,
                completed_date=None,
                tolerance_pct=20,
            )
        )

    def test_non_positive_planned_duration_cannot_be_scored(self):
        self.assertIsNone(
            compute_kpi1_score(
                start_date=self.start,
                due_date=self.start,
                completed=True,
                completed_date=self.start,
                tolerance_pct=20,
            )
        )

    def test_weighted_ratio_gives_larger_plans_more_influence(self):
        ratio = weighted_hours_ratio(
            used_hours=pd.Series([12, 80]),
            planned_hours=pd.Series([8, 80]),
        )
        self.assertEqual(ratio, 1.05)

    def test_weighted_ratio_ignores_missing_and_non_positive_plans(self):
        ratio = weighted_hours_ratio(
            used_hours=pd.Series([12, 50, 25]),
            planned_hours=pd.Series([8, 0, None]),
        )
        self.assertEqual(ratio, 1.5)


if __name__ == "__main__":
    unittest.main()
