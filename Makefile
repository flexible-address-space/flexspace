# Makefile
# rules (always with .out)
# SRC-X.out += abc        # extra source: abc.c
# MOD-X.out += abc        # extra module: abc.c abc.h
# ASM-X.out += abc        # extra assembly: abc.S
# DEP-X.out += abc        # extra dependency: abc
# FLG-X.out += -finline   # extra flags
# LIB-X.out += abc        # extra -labc options

# X.out : xyz.h xyz.c # for extra dependences that are to be compiled/linked.

VPATH += .:c/

# X => X.out
TARGETS += test_flextree test_flexfile test_flexdb
# X => X.c only
SOURCES +=
# X => X.S only
ASSMBLY +=
# X => X.c X.h
MODULES += c/lib c/kv c/ord generic flextree flexfile flexdb
# X => X.h
HEADERS +=

# EXTERNSRC/EXTERNDEP do not belong to this repo.
# extern-src will be linked
EXTERNSRC +=
# extern-dep will not be linked
EXTERNDEP +=

FLG +=
LIB += rt m uring

ifeq ($(LEVELDB),y)
	FLG += -DLEVELDB
	LIB += leveldb
endif
ifeq ($(ROCKSDB),y)
	FLG += -DROCKSDB
	LIB += rocksdb
endif
ifeq ($(LMDB),y)
	FLG += -DLMDB
	LIB += lmdb
endif
ifeq ($(KVELL),y)
	FLG += -DKVELL -L.
	LIB += kvell
endif

# when $ make FORKER_PAPI=y
ifeq ($(strip $(FORKER_PAPI)),y)
LIB += papi
FLG += -DFORKER_PAPI
endif

include c/Makefile.common

libflexfile.so : Makefile Makefile.common c/lib.h c/lib.c flextree.h \
	flextree.c flexfile.h flexfile.c generic.h generic.c wrapper.c wrapper.h
	$(eval ALLFLG := $(CSTD) $(EXTRA) $(FLG) -shared -fPIC)
	$(eval ALLLIB := $(addprefix -l,$(LIB) $(LIB-$@)))
	$(CCC) $(ALLFLG) -o $@ wrapper.c flextree.c flexfile.c generic.c c/lib.c $(ALLLIB) -ldl
