import argparse
import calendar
import csv
import inspect
import json
import os
import re
import time
from tqdm import tqdm

_owner_path_template = os.path.join('{src_dir}', '{owner}')
_repo_path_template = os.path.join('{src_dir}', '{owner}', '{repo}')
_pull_path_template = os.path.join('{src_dir}', '{owner}', '{repo}', 'pull-{pull_number}.json')
_diff_path_template = os.path.join('{src_dir}', '{owner}', '{repo}', 'pull-{pull_number}.diff')
_issue_path_template = os.path.join('{src_dir}', '{owner}', '{repo}', 'issue-{issue_number}.json')

_diff_file_pattern = re.compile(r'^diff --git a\/(?:.*?) b\/(.*?)\n$')
_diff_file_anchor_pattern = re.compile(r'(?:---|\+\+\+) \S*?\/(.*?)$')

_sections = [
    'build/',
    'cli/',
    'extensions/',
    'remote/',
    'resources/',
    'scripts/',
    'src/',
    'test/',
    'other ']

_section_attributes = [
    'changed_files',
    'additions',
    'deletions'
]

_dataset_header = [
    'issue_number',
    'issue_title',
    'issue_created_at',
    'issue_author_id',
    'issue_author_association',
    'issue_labels#Lst',
    'issue_state',
    'issue_state_reason',
    'pull_number',
    'pull_created_at',
    'pull_updated_at',
    'pull_merged_at',
    'pull_comments',
    'pull_review_comments',
    'pull_commits',
    'pull_additions',
    'pull_deletions',
    'pull_changed_files',
    'pull_labels#Lst',
    'pull_milestone#Ord(none,-1,December 2025,January 2026,February 2026,March 2026,April 2026,On Deck,Backlog,Backlog Candidates)',
    'pull_state',
    'pull_locked',
    'pull_draft',
    'pull_merged',
    'pull_mergeable',
    'pull_mergeable_state',
    'pull_rebaseable',
]

_section_headers = [f'pull_section::{s[:-1]}_{a}{r}' for r in ['', '_relative'] for a in _section_attributes for s in _sections]

_dataset_header += _section_headers

_author_association_value = {
    'COLLABORATOR': 0,
    'CONTRIBUTOR': 1,
    'FIRST_TIMER': 2,
    'FIRST_TIME_CONTRIBUTOR': 3,
    'MANNEQUIN': 4,
    'MEMBER': 5,
    'NONE': 6,
    'OWNER': 7,
}

def write_dataset(src_dir,
                  dst_file,
                  limit_rows=0,
                  start_date="2000-01-01",
                  end_date="2050-01-01",
                  probs_file=None):
    """Reads JSON files downloaded by the Crawler and writes a CSV file from their
    data.

    The CSV file will have the following columns:
    - issue_number: Integer
    - issue_title: Text
    - issue_created_at: Integer, in Unix time
    - issue_author_id: Integer
    - issue_author_association: Integer enum (see values below)
    - issue_labels: Comma-separated text, can be empty
    - pull_number: Integer
    - pull_created_at: Integer, in Unix time
    - pull_updated_at: Integer, in Unix time, can be empty
    - pull_merged_at: Integer, in Unix time, can be empty
    - pull_comments: Integer
    - pull_review_comments: Integer
    - pull_commits: Integer
    - pull_additions: Integer
    - pull_deletions: Integer
    - pull_changed_files: Integer
    - pull_labels: Comma-separated text, can be empty
    - pull_milestone: Text
    - pull_state: Text
    - pull_locked: 0 or 1
    - pull_draft: 0 or 1
    - pull_merged: 0 or 1
    - pull_mergeable: 0 or 1
    - pull_mergeable_state: Text
    - pull_rebaseable: 0 or 1
    The value of issue_body_plain is converted from issue_body_md. The conversion is
    not always perfect. In some cases, issue_body_plain still contains some Markdown
    tags.
    The value of issue_author_association can be one of the following:
    - 0: Collaborator
    - 1: Contributor
    - 2: First-timer
    - 3: First-time contributor
    - 4: Mannequin
    - 5: Member
    - 6: None
    - 7: Owner
    Rows are sorted by repository owner username, repository name, pull request
    number, and then issue number.
    The source directory must contain owner/repo/issue-N.json and
    owner/repo/pull-N.json files. The destination directory of Crawler should
    normally be used as the source directory of Writer. The destination file will be
    overwritten if it already exists.

    Args:
        src_dir (str): Source directory.
        dst_file (str): Destination CSV file.
        limit_rows (int): Maximum number of rows to write.
    """
    repo_full_names = []
    repo_num_rows = []
    total_num_rows = 0
    start_date = _iso_to_unix(start_date + "T00:00:00Z")
    end_date = _iso_to_unix(end_date + "T00:00:00Z")
    def print_results():
        for r, n in zip(repo_full_names, repo_num_rows):
            print('{}: {:,}'.format(r, n))
        print('Total: {:,}'.format(total_num_rows))

    probs = None
    if probs_file:
        probs = _read_probs(probs_file)

    with open(dst_file, 'w', newline='') as dataset_file:
        dataset = csv.writer(dataset_file)
        dataset.writerow(_dataset_header if not probs else _dataset_header + [f"pull_topic::{p.replace(' ', '_')}" for p in probs[0][1:]])
        owner_repo_pairs = _sorted_owner_repo_pairs(src_dir)
        num_repos = len(owner_repo_pairs)
        for i, (owner, repo) in enumerate(owner_repo_pairs):
            repo_full_name = '{}/{}'.format(owner, repo)
            repo_full_names.append(repo_full_name)
            repo_num_rows.append(0)
            issue_list = {}
            print('{} ({:,}/{:,})'.format(repo_full_name, i + 1, num_repos))
            for j, pull_number in enumerate(tqdm(_sorted_pull_numbers(src_dir, owner, repo))):
                pull = _read_json(_pull_path_template.format(src_dir=src_dir, owner=owner, repo=repo, pull_number=pull_number))
                if _iso_to_unix(pull['created_at']) < start_date or _iso_to_unix(pull['created_at']) > end_date:
                    continue
                pull['linked_issue_numbers'].sort()

                diff = _read_diff(_diff_path_template.format(src_dir=src_dir, owner=owner, repo=repo, pull_number=pull_number))
                _get_section_changes(pull, diff)
                
                for a in _section_attributes:
                    if sum([pull['section_data'][i][a] for i in range(len(_sections))]) != pull[a]:
                        print([pull['section_data'][i][a] for i in range(len(_sections))])
                        print(sum([pull['section_data'][i][a] for i in range(len(_sections))]))
                        print(pull[a])
                        print(pull_number)

                if probs:
                    pull['topics'] = probs[j + 1][1:]

                for issue_number in pull['linked_issue_numbers']:
                    issue = _read_json(_issue_path_template.format(src_dir=src_dir, owner=owner, repo=repo, issue_number=issue_number))
                    if _iso_to_unix(issue['created_at']) < start_date or _iso_to_unix(issue['created_at']) > end_date:
                        continue
                    issue_list[issue_number] = True
                    dataset.writerow(_dataset_row(issue, pull=pull, probs=probs != None))
                    repo_num_rows[i] += 1
                    total_num_rows += 1
                    if total_num_rows == limit_rows:
                        print('Limit of {:,} rows reached'.format(limit_rows))
                        print_results()
                        return
                    
            for issue_number in tqdm(_sorted_issue_numbers(src_dir, owner, repo)):
                issue = _read_json(_issue_path_template.format(src_dir=src_dir, owner=owner, repo=repo, issue_number=issue_number))
                if issue_number in issue_list or _iso_to_unix(issue['created_at']) < start_date or _iso_to_unix(issue['created_at']) > end_date:
                    continue
                dataset.writerow(_dataset_row(issue, probs=probs != None))
                repo_num_rows[i] += 1
                total_num_rows += 1
                if total_num_rows == limit_rows:
                    print('Limit of {:,} rows reached'.format(limit_rows))
                    print_results()
                    return

    print('Finished')
    print_results()

def _get_section_changes(pull, diff):
    pull['section_data'] = [{a: 0 for a in _section_attributes} for s in _sections]
    current_section = len(_sections) - 1
    current_filename = ''
    for line in diff:
        file_match = _diff_file_pattern.match(line)
        if file_match:
            filename = file_match.group(1)
            if filename != current_filename:
                current_section = next((i for (i, s) in enumerate(_sections) if filename.startswith(s)), len(_sections) - 1)
                pull['section_data'][current_section]['changed_files'] += 1
                current_filename = filename
            continue

        if line.startswith('+') and not _diff_file_anchor_pattern.match(line):
            pull['section_data'][current_section]['additions'] += 1
            continue
        if line.startswith('-') and not _diff_file_anchor_pattern.match(line):
            pull['section_data'][current_section]['deletions'] += 1
            continue

def _sorted_owner_repo_pairs(src_dir):
    pairs = [] # [(owner1,repo1), (owner2,repo2)]
    owners = os.listdir(src_dir)
    owners.sort()
    for owner in owners:
        repos = os.listdir(_owner_path_template.format(src_dir=src_dir, owner=owner))
        repos.sort()
        for repo in repos:
            pairs.append((owner, repo))
    return pairs

def _sorted_pull_numbers(src_dir, owner, repo):
    filenames = os.listdir(_repo_path_template.format(src_dir=src_dir, owner=owner, repo=repo))
    pull_numbers = [int(f[5:-5]) for f in filenames if f.startswith('pull-') and f.endswith('.json')]
    pull_numbers.sort()
    return pull_numbers

def _sorted_issue_numbers(src_dir, owner, repo):
    filenames = os.listdir(_repo_path_template.format(src_dir=src_dir, owner=owner, repo=repo))
    issue_numbers = [int(f[6:-5]) for f in filenames if f.startswith('issue-') and f.endswith('.json')]
    issue_numbers.sort()
    return issue_numbers

def _read_json(path):
    with open(path, 'r') as f:
        return json.load(f)
    
def _read_diff(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.readlines()
    
def _read_probs(path):
    file = []
    with open(path, 'r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            file.append(row)
    return file

def _dataset_row(issue, pull=None, probs=False):
    issue_label_ids = ','.join(str(l['name']) for l in issue['labels'])
    pull_label_ids = ','.join(str(l['name']) for l in pull['labels']) if pull else ''
    section_row_data = [(pull['section_data'][i][a] / max(1, (1 if r == '' else pull[a]))) if pull else '' for r in ['', '_relative'] for a in _section_attributes for i in range(len(_sections))]
    topic_row_data = (pull['topics'] if pull else [0 for _ in range(15)]) if probs else []
    return [
        issue['number'],
        issue['title'],
        _iso_to_unix(issue['created_at']),
        issue['user']['id'],
        _author_association_value[issue['author_association']],
        issue_label_ids,
        issue['state'],
        issue['state_reason'],
        pull['number'] if pull else '',
        _iso_to_unix(pull['created_at']) if pull else -1,
        _iso_to_unix(pull['updated_at']) if pull and pull['updated_at'] else -1,
        _iso_to_unix(pull['merged_at']) if pull and pull['merged_at'] else -1,
        pull['comments'] if pull else '',
        pull['review_comments'] if pull else '',
        pull['commits'] if pull else '',
        pull['additions'] if pull else '',
        pull['deletions'] if pull else '',
        pull['changed_files'] if pull else '',
        pull_label_ids,
        (pull['milestone']['title'] if pull['milestone'] else 'none') if pull else '-1',
        pull['state'] if pull else '',
        (1 if pull['locked'] else 0) if pull else '',
        (1 if pull['draft'] else 0) if pull else '',
        (1 if pull['merged'] else 0) if pull else '',
        (1 if pull['mergeable'] else 0) if pull else '',
        pull['mergeable_state'] if pull else '',
        (1 if pull['rebaseable'] else 0) if pull else '',
    ] + section_row_data + topic_row_data

def _iso_to_unix(iso):
    utc_time = time.strptime(iso, '%Y-%m-%dT%H:%M:%SZ')
    return calendar.timegm(utc_time)

def main():
    crawl_params = inspect.signature(write_dataset).parameters
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Read JSON files downloaded by the Crawler and write a CSV file from their data. '
                    'The source directory must contain owner/repo/issue-N.json and owner/repo/pull-N.json files. '
                    'The destination directory of Crawler should normally be used as the source directory of Writer. '
                    'The destination file will be overwritten if it already exists.')
    parser.add_argument('-l', '--limit-rows', type=int, default=0,
        help='limit number of rows to write, ignored if non-positive')
    parser.add_argument('-e', '--start-date', type=str, default=crawl_params['start_date'].default,
        help='date from which to start the crawl, with pattern YYYY-MM-DD')
    parser.add_argument('-E', '--end-date', type=str, default=crawl_params['end_date'].default,
        help='date at which to end the crawl, with pattern YYYY-MM-DD')
    parser.add_argument('-p', '--probs-file', type=str, default=crawl_params['probs_file'].default,
        help='file including probabilities')
    parser.add_argument('src_dir', type=str,
        help='source directory')
    parser.add_argument('dst_file', type=str,
        help='destination CSV file')
    args = parser.parse_args()
    write_dataset(args.src_dir, args.dst_file, limit_rows=args.limit_rows, start_date=args.start_date, end_date=args.end_date, probs_file=args.probs_file)

if __name__ == '__main__':
    main()
