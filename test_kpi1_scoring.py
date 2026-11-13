import unittest
from datetime import date, timedelta

import pandas as pd

from streamlit_app import (
    aggregate_subtree_values,
    build_indexes,
    compute_completion_adjustments,
    compute_kpi1_score,
    compute_unfinished_row_overrun,
    effort_minutes_total,
    weighted_hours_ratio,
)


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
            as_of=self.start + timedelta(days=200),
        )

    def score_open(self, as_of_days, tolerance_pct=20):
        return compute_kpi1_score(
            start_date=self.start,
            due_date=self.due,
            completed=False,
            completed_date=None,
            tolerance_pct=tolerance_pct,
            as_of=self.start + timedelta(days=as_of_days),
        )

    def test_completion_within_plan_scores_one(self):
        self.assertEqual(self.score_completed(100), 1.0)

    def test_score_decreases_linearly_within_tolerance(self):
        self.assertAlmostEqual(self.score_completed(110), 0.5)

    def test_completion_at_end_of_tolerance_scores_zero(self):
        self.assertEqual(self.score_completed(120), 0.0)

    def test_open_overdue_task_is_scored_as_if_closed_today(self):
        self.assertAlmostEqual(self.score_open(110), 0.5)
        self.assertEqual(self.score_open(120), 0.0)
        self.assertEqual(self.score_open(160), 0.0)

    def test_open_task_due_today_is_not_scored(self):
        self.assertIsNone(self.score_open(100))

    def test_open_task_before_due_date_is_not_scored(self):
        self.assertIsNone(self.score_open(50))

    def test_open_overdue_task_without_start_date_is_not_scored(self):
        self.assertIsNone(
            compute_kpi1_score(
                start_date=None,
                due_date=self.due,
                completed=False,
                completed_date=None,
                tolerance_pct=20,
                as_of=self.start + timedelta(days=110),
            )
        )

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


class PlanAggregationTests(unittest.TestCase):
    def test_completion_adjustments_do_not_net_savings_and_overruns(self):
        df = pd.DataFrame(
            [
                {
                    "completed": True,
                    "planned_hours": 30,
                    "allocated_hours": 20,
                    "used_hours_until_yesterday": 20,
                },
                {
                    "completed": True,
                    "planned_hours": 5,
                    "allocated_hours": 20,
                    "used_hours_until_yesterday": 40,
                },
                {
                    "completed": False,
                    "planned_hours": 100,
                    "allocated_hours": 0,
                    "used_hours_until_yesterday": 0,
                },
            ]
        )
        self.assertEqual(compute_completion_adjustments(df), (-10, 20))

    def test_remaining_plan_is_plan_minus_used(self):
        planned_hours = 590
        used_hours = 282
        self.assertEqual(max(0, planned_hours - used_hours), 308)

    def test_effort_minutes_total_includes_future_daily_allocations(self):
        task = {
            "effortAllocation": {
                "responsibleAllocation": [
                    {
                        "dailyAllocation": [
                            {"date": "2026-11-10", "effortMinutes": 120},
                            {"date": "2026-11-13", "effortMinutes": 180},
                        ]
                    }
                ]
            }
        }
        self.assertEqual(effort_minutes_total(task), 300)

    def test_unfinished_overrun_uses_aggregated_rows_and_stops_at_parent(self):
        rows = {
            "p": {
                "planned_hours": 200,
                "used_hours_until_yesterday": 150,
                "completed": False,
            },
            "a": {
                "planned_hours": 20,
                "used_hours_until_yesterday": 52,
                "completed": False,
            },
            "b": {
                "planned_hours": 40,
                "used_hours_until_yesterday": 60,
                "completed": False,
            },
            "d": {
                "planned_hours": 10,
                "used_hours_until_yesterday": 30,
                "completed": False,
            },
            "e": {
                "planned_hours": 100,
                "used_hours_until_yesterday": 500,
                "completed": False,
            },
        }
        children = {None: ["p"], "p": ["a", "b", "d"], "d": ["e"]}
        self.assertEqual(
            compute_unfinished_row_overrun(rows, children),
            32 + 20 + 20,
        )

    def test_subtree_aggregation_includes_all_descendants_and_separates_rev(self):
        tasks = [
            {
                "id": "core",
                "title": "CORE",
                "customItemTypeId": "core",
                "superTaskIds": [],
            },
            {
                "id": "regular",
                "title": "Regular",
                "customItemTypeId": "",
                "superTaskIds": ["core"],
            },
            {
                "id": "bom",
                "title": "BOM",
                "customItemTypeId": "bom",
                "superTaskIds": ["core"],
            },
            {
                "id": "bom-child",
                "title": "BOM child",
                "customItemTypeId": "",
                "superTaskIds": ["bom"],
            },
            {
                "id": "rev",
                "title": "POPRAWKI",
                "customItemTypeId": "rev",
                "superTaskIds": ["bom"],
            },
            {
                "id": "rev-child",
                "title": "Rev child",
                "customItemTypeId": "",
                "superTaskIds": ["rev"],
            },
        ]
        tasks_by_id, children = build_indexes(tasks)
        raw_planned = {
            "core": 20.0,
            "regular": 5.0,
            "bom": 10.0,
            "bom-child": 2.0,
            "rev": 3.0,
            "rev-child": 4.0,
        }
        alloc = {
            "core": 0,
            "regular": 60,
            "bom": 120,
            "bom-child": 60,
            "rev": 100,
            "rev-child": 40,
        }
        used = {
            "core": 0,
            "regular": 30,
            "bom": 60,
            "bom-child": 30,
            "rev": 50,
            "rev-child": 20,
        }

        totals = aggregate_subtree_values(
            "core",
            tasks_by_id,
            children,
            raw_planned,
            alloc,
            used,
            "rev",
        )
        self.assertEqual(totals["planned_hours"], 37.0)
        self.assertEqual(totals["allocated_minutes"], 240)
        self.assertEqual(totals["used_minutes"], 120)
        self.assertEqual(totals["revisions_allocated_minutes"], 140)


if __name__ == "__main__":
    unittest.main()
