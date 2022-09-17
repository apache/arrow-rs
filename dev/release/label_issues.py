#!/usr/bin/env python

##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

# Python script to add labels to github issues from the PRs that closed them
#
# Required setup:
# $ pip install PyGithub
#
# ARROW_GITHUB_API_TOKEN  needs to be set to your github token
from github import Github
import os
import re



# get all cross referenced issues from the named issue
# (aka linked PRs)
#    issue = arrow_repo.get_issue(issue_number)
def get_cross_referenced_issues(issue):
    all_issues = set()
    for timeline_item in issue.get_timeline():
        if timeline_item.event == 'cross-referenced' and timeline_item.source.type == 'issue':
            all_issues.add(timeline_item.source.issue)

    # convert to list
    return [i for i in all_issues]


# labels not to transfer
BLACKLIST_LABELS = {'development-process', 'api-change'}

# Adds labels to the specified issue with the labels from linked pull requests
def relabel_issue(arrow_repo, issue_number):
    #print(issue_number, 'fetching issue')
    issue = arrow_repo.get_issue(issue_number)
    print('considering issue', issue.html_url)
    linked_issues = get_cross_referenced_issues(issue)
    #print('  ', 'cross referenced issues:', linked_issues)

    # Figure out what labels need to be added, if any
    existing_labels = set()
    for label in issue.labels:
        existing_labels.add(label.name)

    # find all labels to add
    for linked_issue in linked_issues:
        if linked_issue.pull_request is None:
            print('  ', 'not pull request, skipping', linked_issue.html_url)
            continue

        if linked_issue.repository.name != 'arrow-rs':
            print('  ', 'not in arrow-rs, skipping', linked_issue.html_url)
            continue

        print('  ', 'finding labels for linked pr', linked_issue.html_url)
        linked_labels = set()
        for label in linked_issue.labels:
            linked_labels.add(label.name)
            #print('  ', 'existing labels:', existing_labels)

            labels_to_add = linked_labels.difference(existing_labels)

            # remove any blacklist labels, if any
            for l in BLACKLIST_LABELS:
                labels_to_add.discard(l)

            if len(labels_to_add) > 0:
                print('  ', 'adding labels: ', labels_to_add, 'to', issue.number)
                for label in labels_to_add:
                    issue.add_to_labels(label)
                    print('    ', 'added', label)
                    existing_labels.add(label)

                # leave a note about what updated these labels
                issue.create_comment('`label_issue.py` automatically added labels {} from #{}'.format(labels_to_add, linked_issue.number))


# what section headings in the CHANGELOG.md file contain closed issues that may need relabeling
ISSUE_SECTION_NAMES = ['Closed issues:', 'Fixed bugs:', 'Implemented enhancements:']

# find all possible issues / bugs by scraping CHANGELOG.md
#
# TODO: Find all tickets merged since this tag
# The compare api can find all commits since that tag
# I could not find a good way in the github API to find the PRs connected to a commit
#since_tag = '22.0.0'

def find_issues_from_changelog():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(script_dir, '..', '..', 'CHANGELOG.md')

    issues = set()

    # Flag that
    in_issue_section = False

    with open(path, 'r') as f:
        for line in f:
            #print('line: ', line)
            line = line.strip()
            if line.startswith('**'):
                section_name = line.replace('**', '')
                if section_name in ISSUE_SECTION_NAMES:
                    #print('  ', 'is issue section', section_name)
                    in_issue_section = True
                else:
                    #print('  ', 'is not issue section', section_name)
                    in_issue_section = False

            if in_issue_section:
                match = re.search('#([\d]+)', line)
                if match is not None:
                    #print('  ', 'reference', match.group(1))
                    issues.add(match.group(1))

    # Convert to list of number
    return sorted([int(i) for i in issues])


if __name__ == '__main__':
    print('Attempting to label github issues from their corresponding PRs')

    issues = find_issues_from_changelog()
    print('Issues found in CHANGELOG: ', issues)

    github_token = os.environ.get("ARROW_GITHUB_API_TOKEN")

    print('logging into GITHUB...')
    github = Github(github_token)

    print('getting github repo...')
    arrow_repo = github.get_repo('apache/arrow-rs')

    for issue in issues:
        relabel_issue(arrow_repo, issue)
