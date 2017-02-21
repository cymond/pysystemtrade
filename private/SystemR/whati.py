import sys, os

# ------------------------------------------------------------
print("sys.executable: ", sys.executable)
print("os.get_cwd(): ", os.getcwd())
print("sys.version: ", sys.version)
print("sys.path")
print(sys.path)
print("--------------------------")
print("os.path: ", os.path)
print('\n'.join(sys.path))
print("os.path.dirname(__file___): ", os.path.dirname(__file__))
print(os.path.join(os.path.dirname(__file__), '..'))
print("--------------------------")
print("Try this: ", os.path.dirname(os.path.abspath(__file__)))
i = 0
while i < 20:
	print(i + 1, ": Mase is learning to code in Python")
	i = i+1

#print("PYTHONPATH", os.environ["PYTHONPATH"])
