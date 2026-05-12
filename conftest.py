"""Pytest root config — 把 project root 加進 sys.path"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
