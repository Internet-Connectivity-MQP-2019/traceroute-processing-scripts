import sys
from statistics import median, mean, stdev


class SpatialQuadtree:
	def __init__(self, bbox=None, max_items=10, max_depth=20, auto_subdivide=True):
		"""Bbox format: min x, min y, max x, max y"""
		self.bbox = bbox
		self.max_items = max_items
		self.max_depth = max_depth
		self.children = []
		self.items = []
		self.auto_subdivide = auto_subdivide

	def __str__(self):
		return "Items: {}, Children: {}, bbox: {}".format(len(self.items), len(self.children), self.bbox)

	def insert(self, item, point):
		"""Insert an item into the quadtree"""
		# Either there are no items here and we can still insert items, or we've reached max depth; insert to items list
		if (len(self.children) == 0 and len(self.items) < self.max_items) or self.max_depth == 0:
			self.items.append((point, item))
			return

		# Too many items in this node -- split into four based on medians and
		if len(self.items) >= self.max_items:
			self.items.append((point, item))

			return

		# Insert item into the correct child
		self.__find_child(item[0]).insert(item[1], item[0])

	def __subdivide(self):
		"""Split into four children and insert this node's items into the children as appropriate"""
		if len(self.items) == 0:
			return

		# Split calculation -- use the median value for each coordinate,  unless it's the same as one of the box bounds.
		# In that case, fall back to an even split.
		x_list = [item[0][0] for item in self.items]
		y_list = [item[0][1] for item in self.items]

		split_x = median(x_list)
		if split_x == self.bbox[0] or split_x == self.bbox[2]:
			x_min = min(x_list)
			split_x = x_min + (max(x_list) - x_min) / 2
		del x_list

		split_y = median(y_list)
		if split_y == self.bbox[1] or split_y == self.bbox[3]:
			y_min = min(y_list)
			split_y = y_min + (max(y_list) - y_min) / 2
		del y_list

		self.children.append(SpatialQuadtree((self.bbox[0], split_y, split_x, self.bbox[3]), self.max_items, self.max_depth - 1, self.auto_subdivide))  # Top left
		self.children.append(SpatialQuadtree((split_x, split_y, self.bbox[2], self.bbox[3]), self.max_items, self.max_depth - 1, self.auto_subdivide))  # Top right
		self.children.append(SpatialQuadtree((split_x, self.bbox[1], self.bbox[2], split_y), self.max_items, self.max_depth - 1, self.auto_subdivide))  # Bottom right
		self.children.append(SpatialQuadtree((self.bbox[0], self.bbox[1], split_x, split_y), self.max_items, self.max_depth - 1, self.auto_subdivide))  # Bottom left
		for item in self.items:
			self.__find_child(item[0]).insert(item[1], item[0])
		self.items.clear()

	def force_subdivide(self):
		"""For use when auto subdivide is off; this forces nodes in an existing quadtree to subdivide, guaranteeing that
		the constructed quadtree will be balanced"""
		if self.max_depth == 0:
			return
		if len(self.children) == 0:
			self.__subdivide()
		for child in self.children:
			if len(child.items) > self.max_items:
				child.force_subdivide()

	def __find_child(self, point):
		"""Find the child that the given point belongs in"""
		for child in self.children:
			if child.fits_in_bbox(point):
				return child
		print("Point {} doesn't fit in bounding box {}".format(point, self.bbox), file=sys.stderr)

	def fits_in_bbox(self, point):
		"""Determine if a point fits in this tree's bounding box"""
		return self.bbox[0] <= point[0] <= self.bbox[2] and self.bbox[1] <= point[1] <= self.bbox[3]

	def get_bboxes(self):
		"""Get a list of bounding boxes coupled with the average of the items contained within"""
		# Recursively call into children to get their appropriate lowest level bounding boxes
		if len(self.children) > 0:
			results = []
			for child in self.children:
				results.extend(child.get_bboxes())
			return results
		if len(self.items) == 0:
			return [[*self.bbox, 0, 0, 0, 0]]

		# We have items! Get their averages
		vals = [i[1] for i in self.items]
		return [[*self.bbox, mean(vals), stdev(vals) if len(vals) > 1 else float("nan"), median(vals), len(self.items)]] #  trust me on this, it makes .extend work

	def get_bboxes_blank(self):
		"""Get a list of bounding boxes, no data values included. This means ALL children are returned."""
		if len(self.children) > 0:
			results = []
			for child in self.children:
				results.extend(child.get_bboxes_blank())
			return results

		return [[*self.bbox]]
