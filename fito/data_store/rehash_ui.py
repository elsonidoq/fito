from cmd2 import Cmd
from fito import Operation
from fito import Spec
from fito.specs.diff import Diff


class RehashUI(Cmd):
    prompt = "rehash> "
    user_quit = False
    ignored_specs = set()

    def __init__(self, data_store, spec):
        Cmd.__init__(self)
        self.data_store = data_store
        self.spec = spec

        assert spec not in data_store
        self.print_header()
        self.similar_specs = data_store.find_similar(spec)
        print
        print self.colorize("Similar specs", 'green')
        self.do_print('similar_specs')

    def print_header(self):
        print '{} was not found in {}\n'.format(self.colorize('Spec', 'green'), self.colorize('Data store', 'green'))
        print
        print '{:<30}{}'.format(self.colorize('Spec:', 'green'), self.spec)
        print '{:<30}{}\n'.format(self.colorize('Data store:', 'green'), self.data_store)

    def do_print(self, thing):
        """
        Print a thing. Do print <tab> to see available choices
        Also, thing can be any of the indices shown in print similar_specs
        """
        assert thing in 'spec data_store ds similar_specs'.split() or self.is_valid_position(thing, False)

        if self.is_valid_position(thing, False):
            spec, score = self.similar_specs[int(thing) - 1]
            print self.colorize('Spec:', 'green')
            print spec.yaml.dumps()
            print '{}: {}'.format(self.colorize('Similarity', 'green'), score)

        elif thing == 'similar_specs':
            print "{}{:^80}{}".format(*[self.colorize(e, 'green') for e in ('Position', 'Spec', 'score')])
            for i, (spec, score) in enumerate(self.similar_specs[:10]):
                print "{}){:^80}{}".format(i + 1, spec, score)
        else:
            if thing == 'ds': thing = 'data_store'
            print
            print getattr(self, thing).yaml.dumps()

    def complete_print(self, text, line, begidx, endidx):
        alternatives = 'spec data_store ds similar_specs'.split()
        return [e for e in alternatives if e.startswith(text)]

    def do_execute(self):
        """
        Executes the spec and saves it on self.data_store
        """
        if not isinstance(self.spec, Operation):
            print "Can only execute operations"
            print "{} is instance of class {}".format(self.spec, type(self.spec).__name__)
        else:
            self.data_store[self.spec] = self.spec.execute()

    def is_valid_position(self, position, show_message=True):
        if not position.isdigit() or int(position) > 10 or int(position) < 1:
            if show_message:
                print "Invalid value for position '{}'. Expected an integer between 1 and 10".format(position)
            return False
        return True

    def do_rehash(self, action, position):
        assert action in 'copy move'.split()
        if not self.is_valid_position(position): return

        position = int(position) - 1
        target_spec, _ = self.similar_specs[position]

        print '{} from {}'.format(
            self.colorize('Copying' if action == 'copy' else 'Moving', 'green'),
            target_spec
        )

        self.data_store[self.spec] = target_spec
        if action == 'move':
            self.data_store.remove(target_spec)

        print self.colorize('Done!', 'green')
        return self.do_end()

    def do_diff(self, position):
        if not self.is_valid_position(position): return
        position = int(position) - 1
        target_spec, _ = self.similar_specs[position]

        target_spec_dict = target_spec.to_dict() if isinstance(target_spec, Spec) else target_spec
        print
        print Diff.build(self.spec.to_dict(), target_spec_dict)
        print

    def do_copy(self, position):
        """
        Copies the contents of spec refered by `position` to self.data_store[self.spec]
        Execute `print similar_specs` to see candidates
        """
        return self.do_rehash('copy', position)

    def do_move(self, position):
        """
        Moves the contents of spec refered by `position` to self.data_store[self.spec]
        Execute `print similar_specs` to see candidates
        """
        return self.do_rehash('move', position)

    def do_quit(self, arg=None):
        print "Quitting..."
        RehashUI.user_quit = True
        return Cmd.do_quit(self, arg)

    def do_end(self, arg=None):
        print "Quitting..."
        return Cmd.do_quit(self, arg)

    def do_ignore(self, arg=None):
        print "Ignoring {}".format(self.spec)
        RehashUI.ignored_specs.add(self.spec)
        return Cmd.do_quit(self, arg)

    def cmdloop(self, intro=None):
        if RehashUI.user_quit:
            raise Quit()
        else:
            Cmd.cmdloop(self, intro)


class Quit(Exception): pass
