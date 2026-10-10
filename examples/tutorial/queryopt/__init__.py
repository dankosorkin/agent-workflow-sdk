# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared foundation for the applied query-optimizer tutorial.

Every chapter imports from here so the schema, the seeded data, the query under
optimization, and the deterministic SQLite scorer are defined once. Nothing in
this package depends on agentflow — it is the "problem", not the "optimizer".
"""
