from __future__ import absolute_import
import os
from tempfile import mkdtemp
from shutil import rmtree
from itertools import count, izip
from git.repo import Repo
from git.objects import Blob
from kenja.parser import BlobParser
from kenja.language import is_target_blob, extension_dict
from kenja.git.util import get_reversed_topological_ordered_commits
from kenja.committer import SyntaxTreesCommitter
from logging import getLogger

logger = getLogger(__name__)


class HistorageConverter:
    def __init__(self, org_git_repo_dir, historage_dir, syntax_trees_dir=None):
	logger.info('starting...')
        if org_git_repo_dir:
            self.org_repo = Repo(org_git_repo_dir)

        self.check_and_make_working_dir(historage_dir, False)
        self.historage_dir = historage_dir

        self.use_tempdir = syntax_trees_dir is None
	self.skip_parsing = False
	self.non_contiguous_parents = set()
        if self.use_tempdir:
	    tmpdir = mkdtemp()
            self.syntax_trees_dir = os.path.join(tmpdir, 'blobs')
	    os.mkdir(self.syntax_trees_dir)
            logger.info(tmpdir)
        else:
            self.check_and_make_working_dir(syntax_trees_dir, False)
	    working_dir = os.path.join(syntax_trees_dir, 'blobs')
            if os.listdir(syntax_trees_dir):
		self.skip_parsing = True
	        syntax_trees_dir = working_dir
	    else:
	        syntax_trees_dir = working_dir
		os.mkdir(syntax_trees_dir)
            self.syntax_trees_dir = syntax_trees_dir

        self.num_commits = 0

        self.is_bare_repo = False

	self.head_name = self.org_repo.active_branch.name

    def check_and_make_working_dir(self, path, check_is_empty=True):
        if os.path.isdir(path):
            if check_is_empty and os.listdir(path):
                raise Exception('{0} is not an empty directory'.format(path))
        else:
            try:
                os.mkdir(path)
            except OSError:
                logger.error('Kenja cannot make a directory: {0}'.format(path))
                raise

    def parse_all_target_files(self):
        logger.info('create parser processes...')
	if not self.skip_parsing:
            blob_parser = BlobParser(extension_dict, self.syntax_trees_dir, self.org_repo.git_dir)
        parsed_blob = set()
	last_commit_hexsha = ""
        for commit in get_reversed_topological_ordered_commits(self.org_repo, self.org_repo.refs):
            self.num_commits = self.num_commits + 1
	    if self.num_commits % 1000 == 0:
		logger.info('parsed %d commits...' % (self.num_commits))
            if commit.parents:
                for p in commit.parents:
		    if p.hexsha != last_commit_hexsha:
		        self.non_contiguous_parents.add(p.hexsha)
		    if self.skip_parsing:
		        continue
                    for diff in p.diff(commit):
                        if is_target_blob(diff.b_blob):
                            if diff.b_blob.hexsha not in parsed_blob:
                                blob_parser.parse_blob(diff.b_blob)
                                parsed_blob.add(diff.b_blob.hexsha)
            else:
		if self.skip_parsing:
		    continue
                for entry in commit.tree.traverse():
                    if isinstance(entry, Blob) and is_target_blob(entry):
                        if entry.hexsha not in parsed_blob:
                            blob_parser.parse_blob(entry)
                            parsed_blob.add(entry.hexsha)
	    last_commit_hexsha = commit.hexsha
	logger.info('Found %d non-contiguous parents' % (len(self.non_contiguous_parents)))
	if not self.skip_parsing:
            logger.info('waiting parser processes')
            blob_parser.join()

    def prepare_historage_repo(self):
        historage_repo = Repo.init(self.historage_dir, bare=self.is_bare_repo)
        self.set_git_config(historage_repo)
        return historage_repo

    def set_git_config(self, repo):
        reader = repo.config_reader()  # global config
        writer = repo.config_writer()  # local config
        user_key = 'user'
        if not reader.has_option(user_key, 'name'):
            if not writer.has_section(user_key):
                writer.add_section(user_key)
            writer.set(user_key, 'name', 'Kenja Converter')
        if not reader.has_option(user_key, 'email'):
            if not writer.has_section(user_key):
                writer.add_section(user_key)
            writer.set(user_key, 'email', 'default@example.com')

    def convert(self):
	# if not self.skip_parsing:
	self.parse_all_target_files()
	#else:
        #    logger.info('skipping parser...')
        self.construct_historage()

    def construct_historage(self):
        logger.info('convert a git repository to a  historage...')

        historage_repo = self.prepare_historage_repo()
	resume = False
	if len(os.listdir(self.historage_dir)) > 1:
	    kwargs = ['show', historage_repo.head.commit.hexsha]
	    resume_from = historage_repo.git.notes(kwargs)
	    resume = True
        committer = SyntaxTreesCommitter(Repo(self.org_repo.git_dir), historage_repo, self.syntax_trees_dir)
	committer.set_non_contiguous_parents(self.non_contiguous_parents)
        num_commits = self.num_commits if self.num_commits != 0 else '???'
        for head in self.org_repo.heads:
	    if head.name == self.head_name:
                head_hexsha = head.commit.hexsha
        for num, commit in izip(count(), get_reversed_topological_ordered_commits(self.org_repo, self.org_repo.refs)):
            logger.info('[%d/%s] convert %s to: %s' % (num, num_commits, commit.hexsha, historage_repo.git_dir))
	    if resume:
		if commit.hexsha == resume_from:
		    committer.load_commit(commit, historage_repo.head.commit)
		    resume = False
		    exit()
	    else:
                committer.apply_change(commit, head_hexsha == commit.hexsha)
        committer.create_heads()
        committer.create_tags()
        if not self.is_bare_repo:
            historage_repo.head.reset(working_tree=True)
        logger.info('completed!')

    def __del__(self):
        if self.use_tempdir and os.path.exists(self.syntax_trees_dir):
            rmtree(self.syntax_trees_dir)
